package e2e

import (
	"context"
	"fmt"
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
)

const (
	fullLossFSPath    = "/spdkvol/fullloss-marker"
	fullLossBlockPath = "/dev/spdkblk"
)

type fullLossMode struct {
	name   string
	block  bool
	fsType string // "" for raw block
}

var _ = ginkgo.Describe("SPDKCSI-RECONNECT-FULLLOSS", func() {
	f := newTestFramework("spdkcsi")

	ginkgo.Context("volume recovers after total NVMe-oF path loss", func() {
		modes := []fullLossMode{
			{name: "raw block", block: true},
			{name: "ext4 filesystem", fsType: "ext4"},
			{name: "xfs filesystem", fsType: "xfs"},
		}

		for _, m := range modes {
			m := m
			ginkgo.It(fmt.Sprintf("reconnects and keeps data for a %s volume", m.name), func() {
				ns := f.Namespace.Name
				appLabel := "fullloss"
				pvcName := "fullloss-pvc"
				depName := "fullloss"
				marker := "fullloss-" + ns
				if len(marker) > 60 {
					marker = marker[:60]
				}

				ginkgo.By("check node DaemonSet is ready")
				framework.ExpectNoError(waitForNodeServerReady(f.ClientSet, 3*time.Minute), "node DaemonSet ready")

				ginkgo.By("create the StorageClass / PVC for this volume type")
				scName := storageClassName
				if !m.block {
					scName = fmt.Sprintf("fullloss-%s-%s", m.fsType, ns)
					createStorageClassWithParams(f.ClientSet, scName, map[string]string{"csi.storage.k8s.io/fstype": m.fsType})
					ginkgo.DeferCleanup(func() { deleteStorageClass(f.ClientSet, scName) })
				}
				framework.ExpectNoError(createModePVC(f.ClientSet, ns, pvcName, scName, m.block), "create PVC")

				ginkgo.By("pick a worker node and run a pod pinned to it")
				workerNode, _, _ := anyNodePluginPod(f.ClientSet)
				framework.ExpectNoError(
					createPinnedDeployment(f.ClientSet, ns, depName, appLabel, pvcName, workerNode, m.block),
					"create workload")
				ginkgo.DeferCleanup(func() {
					_ = f.ClientSet.AppsV1().Deployments(ns).Delete(context.Background(), depName, metav1.DeleteOptions{})
				})

				pod := waitForReadyPod(f.ClientSet, ns, appLabel, "", 5*time.Minute)

				ginkgo.By("write a marker to the volume")
				writeMarker(f, ns, appLabel, m, marker)

				ginkgo.By("locate the csi-node pod and the volume's NVMe subsystem")
				pluginPod, pluginContainer := nodePluginPodOnNode(f.ClientSet, workerNode)
				lvolID := lvolIDForPVC(f.ClientSet, ns, pvcName)
				sub := waitForSubsystem(f, pluginPod, pluginContainer, lvolID, time.Minute)

				ginkgo.By("induce TOTAL path loss by disconnecting the whole subsystem")
				execInPod(f, driverNamespace(), pluginPod, pluginContainer, "nvme disconnect -n "+sub.NQN)

				ginkgo.By("force-delete the pod to trigger a same-node replacement")
				// GracePeriod 0 mimics the guardian's restart and biases toward the
				// race where the new pod's NodePublish runs before kubelet unstages.
				zero := int64(0)
				framework.ExpectNoError(
					f.ClientSet.CoreV1().Pods(ns).Delete(context.Background(), pod.Name, metav1.DeleteOptions{GracePeriodSeconds: &zero}),
					"force-delete pod %s", pod.Name)

				ginkgo.By("wait for the replacement pod to become ready (volume restaged/reconnected)")
				newPod := waitForReadyPod(f.ClientSet, ns, appLabel, string(pod.UID), 5*time.Minute)
				gomega.Expect(newPod.UID).NotTo(gomega.Equal(pod.UID), "expected a replacement pod")

				ginkgo.By("verify the marker written before the outage is intact")
				gomega.Expect(readMarker(f, ns, appLabel, m, len(marker))).To(gomega.ContainSubstring(marker),
					"data lost across full path loss + recovery")
			})
		}
	})
})

// createModePVC creates an RWO PVC in Filesystem or Block mode.
func createModePVC(c kubernetes.Interface, ns, pvcName, scName string, block bool) error {
	volMode := corev1.PersistentVolumeFilesystem
	if block {
		volMode = corev1.PersistentVolumeBlock
	}
	_, err := c.CoreV1().PersistentVolumeClaims(ns).Create(context.Background(), &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: pvcName},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &scName,
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			VolumeMode:       &volMode,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
			},
		},
	}, metav1.CreateOptions{})
	return err
}

// createPinnedDeployment runs a single alpine pod (via a Deployment so it is
// recreated after deletion) pinned to nodeName, consuming pvcName as a block
// device or filesystem mount.
func createPinnedDeployment(c kubernetes.Interface, ns, name, appLabel, pvcName, nodeName string, block bool) error {
	replicas := int32(1)
	container := corev1.Container{
		Name:    "alpine",
		Image:   "alpine:3",
		Command: []string{"sleep", "365d"},
	}
	if block {
		container.VolumeDevices = []corev1.VolumeDevice{{Name: "vol", DevicePath: fullLossBlockPath}}
	} else {
		container.VolumeMounts = []corev1.VolumeMount{{Name: "vol", MountPath: "/spdkvol"}}
	}

	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": appLabel}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": appLabel}},
				Spec: corev1.PodSpec{
					NodeName:   nodeName,
					Containers: []corev1.Container{container},
					Volumes: []corev1.Volume{{
						Name: "vol",
						VolumeSource: corev1.VolumeSource{
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvcName},
						},
					}},
				},
			},
		},
	}
	_, err := c.AppsV1().Deployments(ns).Create(context.Background(), dep, metav1.CreateOptions{})
	return err
}

// waitForReadyPod waits for a Running+Ready pod matching app=appLabel whose UID
// differs from excludeUID (pass "" to accept any).
func waitForReadyPod(c kubernetes.Interface, ns, appLabel, excludeUID string, timeout time.Duration) *corev1.Pod {
	var ready *corev1.Pod
	gomega.Eventually(func() bool {
		pods, err := c.CoreV1().Pods(ns).List(context.Background(), metav1.ListOptions{LabelSelector: "app=" + appLabel})
		if err != nil {
			return false
		}
		for i := range pods.Items {
			p := &pods.Items[i]
			if string(p.UID) == excludeUID || p.DeletionTimestamp != nil {
				continue
			}
			if p.Status.Phase == corev1.PodRunning && podReady(p) {
				ready = p
				return true
			}
		}
		return false
	}, timeout, 5*time.Second).Should(gomega.BeTrue(), "no ready pod for app=%s", appLabel)
	return ready
}

func podReady(p *corev1.Pod) bool {
	for _, cond := range p.Status.Conditions {
		if cond.Type == corev1.PodReady {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return false
}

// writeMarker writes marker to the volume: to a file for filesystem volumes, or
// to the start of the raw device for block volumes.
func writeMarker(f *framework.Framework, ns, appLabel string, m fullLossMode, marker string) {
	opt := metav1.ListOptions{LabelSelector: "app=" + appLabel}
	if m.block {
		execCommandInPod(f, fmt.Sprintf("printf '%%s' '%s' | dd of=%s bs=4096 count=1 conv=fsync 2>/dev/null", marker, fullLossBlockPath), ns, &opt)
		return
	}
	execCommandInPod(f, fmt.Sprintf("printf '%%s' '%s' > %s && sync", marker, fullLossFSPath), ns, &opt)
}

// readMarker reads back the marker written by writeMarker.
func readMarker(f *framework.Framework, ns, appLabel string, m fullLossMode, length int) string {
	opt := metav1.ListOptions{LabelSelector: "app=" + appLabel}
	if m.block {
		out, _ := execCommandInPod(f, fmt.Sprintf("dd if=%s bs=1 count=%d 2>/dev/null", fullLossBlockPath, length), ns, &opt)
		return out
	}
	out, _ := execCommandInPod(f, "cat "+fullLossFSPath, ns, &opt)
	return out
}