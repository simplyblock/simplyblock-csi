package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"

	"github.com/spdk/spdk-csi/pkg/kubernetes/volumehandle"
)

// nvme list-subsys -o json output (subset of fields we need). The command
// returns a top-level array, one entry per host.
type nvmeSubsysHost struct {
	Subsystems []nvmeSubsystem `json:"Subsystems"`
}

type nvmeSubsystem struct {
	Name  string     `json:"Name"`
	NQN   string     `json:"NQN"`
	Paths []nvmePath `json:"Paths"`
}

type nvmePath struct {
	Name     string `json:"Name"` // controller, e.g. "nvme0"
	State    string `json:"State"`
	ANAState string `json:"ANAState"`
}

var _ = ginkgo.Describe("SPDKCSI-RECONNECT", func() {
	f := newTestFramework("spdkcsi")

	ginkgo.Context("NVMe-oF path recovery for PV/PVC-managed volumes", func() {
		// A PV/PVC-managed volume is connected over multiple NVMe-oF paths. When
		// one path degrades (a controller is deleted on the node, leaving at least
		// one live path so I/O continues), the node plugin's monitor must
		// automatically reconnect the lost path without disrupting the workload.
		// We write data, drop one path, and confirm the monitor restores the full
		// path count and that the data written before the disruption is intact.
		// The mirror-image SPDKCSI-RECONNECT-UNMANAGED test shows the monitor does
		// NOT do this for a volume with no PV/PVC.
		ginkgo.It("restores a degraded managed volume's NVMe paths", func() {
			ns := f.Namespace.Name
			pvcLabel := metav1.ListOptions{LabelSelector: "app=spdkcsi-pvc"}
			const dataPath = "/spdkvol/reconnect-test"
			const data = "reconnect-survives-path-loss"

			ginkgo.By("check driver components are running")
			framework.ExpectNoError(waitForControllerReady(f.ClientSet, 4*time.Minute), "controller ready")
			framework.ExpectNoError(waitForNodeServerReady(f.ClientSet, 3*time.Minute), "node DaemonSet ready")

			ginkgo.By("create a StorageClass pinned to the live cluster, PVC and test pod")
			// Use a test-owned StorageClass pinned to the live cluster rather than
			// the operator's default SC, which may reference a stale cluster_id.
			scName := fmt.Sprintf("reconnect-%s", ns)
			createStorageClassWithParams(f.ClientSet, scName, map[string]string{"cluster_id": liveClusterID(f)})
			ginkgo.DeferCleanup(func() { deleteStorageClass(f.ClientSet, scName) })
			framework.ExpectNoError(createModePVC(f.ClientSet, ns, "spdkcsi-pvc", scName, false), "create PVC")
			framework.ExpectNoError(createFilesystemTestPod(f.ClientSet, ns, testPodName, "spdkcsi-pvc", "spdkcsi-pvc"), "create test pod")
			ginkgo.DeferCleanup(func() { deletePVCAndTestPod(ns) })
			framework.ExpectNoError(
				waitForTestPodReady(f.ClientSet, 5*time.Minute, ns, testPodName),
				"wait for test pod",
			)

			ginkgo.By("write data so we can verify I/O survives the disruption")
			writeDataToPod(f, ns, &pvcLabel, data, dataPath)

			ginkgo.By("locate the worker node and its csi-node pod")
			workerNode := testPodNode(f.ClientSet, ns, testPodName)
			pluginPod, pluginContainer := nodePluginPodOnNode(f.ClientSet, workerNode)
			framework.Logf("test pod on node %q served by csi-node pod %q", workerNode, pluginPod)

			ginkgo.By("resolve the volume's lvol ID from its PV handle")
			lvolID := lvolIDForPVC(f.ClientSet, ns, "spdkcsi-pvc")

			ginkgo.By("find the NVMe subsystem and its live paths on the node")
			sub := waitForSubsystem(f, pluginPod, pluginContainer, lvolID, time.Minute)
			origLive := liveControllers(sub)
			framework.Logf("lvol %s subsystem %s has live paths: %v", lvolID, sub.NQN, origLive)

			if len(origLive) < 2 {
				ginkgo.Skip(fmt.Sprintf(
					"volume has %d live path(s); the monitor only recovers degraded multipath, "+
						"so path-loss recovery cannot be exercised on a single-path volume", len(origLive)))
			}

			ginkgo.By("drop one NVMe path by deleting its controller on the node")
			// Delete the last live controller, leaving at least one path so I/O continues.
			victim := origLive[len(origLive)-1]
			execInPod(f, driverNamespace(), pluginPod, pluginContainer,
				fmt.Sprintf("echo 1 > /sys/class/nvme/%s/delete_controller", victim))

			ginkgo.By("confirm the path count actually dropped")
			gomega.Eventually(func() int {
				s := subsystemForLvol(listSubsystems(f, pluginPod, pluginContainer), lvolID)
				if s == nil {
					return 0
				}
				return len(liveControllers(s))
			}, 30*time.Second, 2*time.Second).Should(gomega.BeNumerically("<", len(origLive)),
				"expected a path to drop after deleting controller %s", victim)

			ginkgo.By("wait for the node plugin to reconnect the missing path")
			gomega.Eventually(func() int {
				s := subsystemForLvol(listSubsystems(f, pluginPod, pluginContainer), lvolID)
				if s == nil {
					return 0
				}
				return len(liveControllers(s))
			}, 3*time.Minute, 3*time.Second).Should(gomega.BeNumerically(">=", len(origLive)),
				"monitor should restore the degraded path back to %d live paths", len(origLive))

			ginkgo.By("verify the data written before the disruption is intact")
			compareDataInPod(f, ns, &pvcLabel, []string{data}, []string{dataPath})
		})
	})
})

// driverNamespace returns the namespace the CSI driver components run in.
func driverNamespace() string {
	if operatorMode {
		return systemNamespace
	}
	return nameSpace
}

// testPodNode returns the node a pod is scheduled on.
func testPodNode(c kubernetes.Interface, ns, podName string) string {
	pod, err := c.CoreV1().Pods(ns).Get(context.Background(), podName, metav1.GetOptions{})
	framework.ExpectNoError(err, "get pod %s/%s", ns, podName)
	gomega.Expect(pod.Spec.NodeName).NotTo(gomega.BeEmpty(), "pod %s has no node assigned", podName)
	return pod.Spec.NodeName
}

// nodePluginPodOnNode returns the csi-node DaemonSet pod (and its first
// container) running on the given node.
func nodePluginPodOnNode(c kubernetes.Interface, nodeName string) (podName, container string) {
	dns := driverNamespace()
	ds, err := c.AppsV1().DaemonSets(dns).Get(context.Background(), nodeDsName, metav1.GetOptions{})
	framework.ExpectNoError(err, "get node DaemonSet %s/%s", dns, nodeDsName)

	pods, err := c.CoreV1().Pods(dns).List(context.Background(), metav1.ListOptions{
		LabelSelector: labels.Set(ds.Spec.Selector.MatchLabels).String(),
		FieldSelector: "spec.nodeName=" + nodeName,
	})
	framework.ExpectNoError(err, "list csi-node pods on node %s", nodeName)
	gomega.Expect(pods.Items).NotTo(gomega.BeEmpty(), "no csi-node pod found on node %s", nodeName)

	pod := pods.Items[0]
	return pod.Name, pluginContainerName(&pod)
}

// lvolIDForPVC resolves the lvol (volume) ID from the PVC's bound PV handle.
func lvolIDForPVC(c kubernetes.Interface, ns, pvcName string) string {
	pvc, err := c.CoreV1().PersistentVolumeClaims(ns).Get(context.Background(), pvcName, metav1.GetOptions{})
	framework.ExpectNoError(err, "get PVC %s/%s", ns, pvcName)
	gomega.Expect(pvc.Spec.VolumeName).NotTo(gomega.BeEmpty(), "PVC %s not bound", pvcName)

	pv, err := c.CoreV1().PersistentVolumes().Get(context.Background(), pvc.Spec.VolumeName, metav1.GetOptions{})
	framework.ExpectNoError(err, "get PV %s", pvc.Spec.VolumeName)
	gomega.Expect(pv.Spec.CSI).NotTo(gomega.BeNil(), "PV %s has no CSI source", pv.Name)

	vh, ok := volumehandle.Parse(pv.Spec.CSI.VolumeHandle)
	gomega.Expect(ok).To(gomega.BeTrue(), "parse volume handle %q", pv.Spec.CSI.VolumeHandle)
	return vh.VolumeID
}

// execInPod runs cmd in a specific pod/container and returns stdout.
func execInPod(f *framework.Framework, ns, podName, container, cmd string) string {
	opts := e2epod.ExecOptions{
		Command:            []string{"/bin/sh", "-c", cmd},
		PodName:            podName,
		Namespace:          ns,
		ContainerName:      container,
		CaptureStdout:      true,
		CaptureStderr:      true,
		PreserveWhitespace: true,
	}
	stdout, stderr, err := e2epod.ExecWithOptions(f, opts)
	if stderr != "" {
		framework.Logf("exec %q stderr: %s", cmd, stderr)
	}
	framework.ExpectNoError(err, "exec %q in %s/%s", cmd, ns, podName)
	return stdout
}

// listSubsystems runs `nvme list-subsys` in the csi-node pod and parses it.
func listSubsystems(f *framework.Framework, podName, container string) []nvmeSubsystem {
	out := execInPod(f, driverNamespace(), podName, container, "nvme list-subsys -o json")
	var hosts []nvmeSubsysHost
	if err := json.Unmarshal([]byte(out), &hosts); err != nil {
		framework.Failf("parse nvme list-subsys output %q: %v", out, err)
	}
	var subs []nvmeSubsystem
	for _, h := range hosts {
		subs = append(subs, h.Subsystems...)
	}
	return subs
}

// subsystemForLvol returns the subsystem whose NQN carries the given lvol ID.
func subsystemForLvol(subs []nvmeSubsystem, lvolID string) *nvmeSubsystem {
	for i := range subs {
		if strings.Contains(subs[i].NQN, lvolID) {
			return &subs[i]
		}
	}
	return nil
}

// waitForSubsystem polls until the subsystem for lvolID appears on the node.
func waitForSubsystem(f *framework.Framework, podName, container, lvolID string, timeout time.Duration) *nvmeSubsystem {
	var found *nvmeSubsystem
	gomega.Eventually(func() *nvmeSubsystem {
		found = subsystemForLvol(listSubsystems(f, podName, container), lvolID)
		return found
	}, timeout, 3*time.Second).ShouldNot(gomega.BeNil(),
		"NVMe subsystem for lvol %s never appeared on the node", lvolID)
	return found
}

// liveControllers returns the controller names of all live paths in a subsystem.
func liveControllers(sub *nvmeSubsystem) []string {
	var ctrls []string
	for _, p := range sub.Paths {
		if strings.EqualFold(p.State, "live") {
			ctrls = append(ctrls, p.Name)
		}
	}
	return ctrls
}
