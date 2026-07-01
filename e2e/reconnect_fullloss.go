package e2e

import (
	"context"
	"fmt"
	"strings"
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
		// Unlike SPDKCSI-RECONNECT (which drops ONE of several paths and lets the
		// monitor reconnect it in place), this exercises TOTAL path loss: the whole
		// NVMe-oF subsystem is disconnected, so the kernel removes the device and
		// the in-place mount is dead — it can only recover when the pod is restarted
		// and the volume is restaged on the replacement pod's NodePublish.
		//
		// In production the guardian is what restarts the pod after detecting the
		// broken lvol. The test does NOT wait for that; it force-deletes the pod
		// itself (grace period 0) to stand in for the guardian's restart — this
		// keeps the test deterministic (no wait for the guardian's poll cycle) and
		// biases toward the race where the replacement pod's NodePublish runs before
		// kubelet unstages the old mount. We then verify the replacement pod comes
		// up with the volume restaged and usable again.
		//
		// Because an unclean total loss can roll back the filesystem journal, we
		// assert the volume is writable+readable again rather than that the exact
		// pre-outage marker survived. Run for raw block, ext4 and xfs, since each
		// filesystem behaves differently when its backing device disappears.
		modes := []fullLossMode{
			{name: "raw block", block: true},
			{name: "ext4 filesystem", fsType: "ext4"},
			{name: "xfs filesystem", fsType: "xfs"},
		}

		for _, m := range modes {
			m := m
			ginkgo.It(fmt.Sprintf("reconnects and keeps data for a %s volume", m.name), func() {
				w := setupManagedWorkload(f, m, "fullloss")
				w.induceTotalPathLoss(f)

				ginkgo.By("force-delete the pod to trigger a same-node replacement")
				// GracePeriod 0 mimics the guardian's restart and biases toward the
				// race where the new pod's NodePublish runs before kubelet unstages.
				zero := int64(0)
				framework.ExpectNoError(
					f.ClientSet.CoreV1().Pods(w.ns).Delete(context.Background(), w.pod.Name, metav1.DeleteOptions{GracePeriodSeconds: &zero}),
					"force-delete pod %s", w.pod.Name)

				ginkgo.By("wait for the replacement pod to restage the mount and make the volume usable")
				// Total path loss leaves the in-place mount dead (I/O error); it
				// recovers when the replacement pod's NodePublish restages the volume.
				// Poll until it is writable+readable again. The generous timeout also
				// covers the fallback where the guardian, not the test's force-delete,
				// drives the restart on its poll cycle (default 5m). We assert
				// usability rather than that the pre-outage marker survived: an unclean
				// total loss can roll back the journal.
				token := "recovered-" + w.ns
				gomega.Eventually(func() error {
					return verifyVolumeUsableE(f, w.ns, w.appLabel, m, token)
				}, 12*time.Minute, 10*time.Second).Should(gomega.Succeed(),
					"volume not usable after full path loss + restage")
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
					// Pin to the node via affinity (not NodeName) so the pod still
					// goes through the scheduler. With WaitForFirstConsumer
					// StorageClasses the scheduler is what stamps the
					// volume.kubernetes.io/selected-node annotation that triggers
					// provisioning; bypassing it with NodeName leaves the PVC Pending.
					Affinity: &corev1.Affinity{
						NodeAffinity: &corev1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
								NodeSelectorTerms: []corev1.NodeSelectorTerm{{
									MatchFields: []corev1.NodeSelectorRequirement{{
										Key:      "metadata.name",
										Operator: corev1.NodeSelectorOpIn,
										Values:   []string{nodeName},
									}},
								}},
							},
						},
					},
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

// verifyVolumeUsableE writes a fresh token to the volume and reads it back from
// the current app=appLabel pod, returning an error if the volume is not usable
// (e.g. still on a dead mount, or no ready pod). It proves the volume recovered
// without relying on pre-outage data surviving an unclean total path loss, which
// can roll back the ext4/xfs journal.
func verifyVolumeUsableE(f *framework.Framework, ns, appLabel string, m fullLossMode, token string) error {
	opt := metav1.ListOptions{LabelSelector: "app=" + appLabel}
	writeCmd := fmt.Sprintf("printf '%%s' '%s' > %s && sync", token, fullLossFSPath)
	readCmd := "cat " + fullLossFSPath
	if m.block {
		writeCmd = fmt.Sprintf("printf '%%s' '%s' | dd of=%s bs=4096 count=1 conv=fsync 2>/dev/null", token, fullLossBlockPath)
		readCmd = fmt.Sprintf("dd if=%s bs=1 count=%d 2>/dev/null", fullLossBlockPath, len(token))
	}
	if _, _, err := execCommandInPodE(f, writeCmd, ns, &opt); err != nil {
		return err
	}
	out, _, err := execCommandInPodE(f, readCmd, ns, &opt)
	if err != nil {
		return err
	}
	if !strings.Contains(out, token) {
		return fmt.Errorf("read back %q, want substring %q", out, token)
	}
	return nil
}
