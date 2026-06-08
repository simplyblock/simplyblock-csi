package e2e

import (
	"context"
	"strings"
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/test/e2e/framework"
)

var _ = ginkgo.Describe("SPDKCSI-FILESYSTEM", func() {
	f := newTestFramework("spdkcsi")

	// -------------------------------------------------------------------------
	// XFS filesystem
	// -------------------------------------------------------------------------

	// A unique StorageClass name is generated per test so parallel runs do not
	// collide.  The SC is created by copying the default one and overriding
	// csi.storage.k8s.io/fstype to "xfs".
	ginkgo.It("XFS volume is provisioned and data persists across pod restarts", func() {
		ns := f.Namespace.Name
		const xfsSC = "spdkcsi-e2e-xfs"

		ginkgo.By("create XFS StorageClass derived from the default one")
		createStorageClassWithParams(f.ClientSet, xfsSC, map[string]string{
			"csi.storage.k8s.io/fstype": "xfs",
		})
		ginkgo.DeferCleanup(func() { deleteStorageClass(f.ClientSet, xfsSC) })

		ginkgo.By("create PVC using XFS StorageClass")
		framework.ExpectNoError(
			createPVC(f.ClientSet, ns, "spdkcsi-pvc-xfs", xfsSC, 1<<30),
			"create XFS PVC",
		)
		ginkgo.DeferCleanup(func() {
			framework.ExpectNoError(
				f.ClientSet.CoreV1().PersistentVolumeClaims(ns).
					Delete(context.Background(), "spdkcsi-pvc-xfs", metav1.DeleteOptions{}),
			)
		})

		ginkgo.By("create pod that mounts the XFS volume")
		framework.ExpectNoError(
			createPodForPVC(f.ClientSet, ns, "spdkcsi-test-xfs", "spdkcsi-pvc-xfs"),
			"create XFS test pod",
		)
		xfsPodLabel := metav1.ListOptions{LabelSelector: "app=spdkcsi-test-xfs"}
		ginkgo.DeferCleanup(func() {
			deletePodByName(f.ClientSet, ns, "spdkcsi-test-xfs")
		})

		ginkgo.By("wait for XFS pod to be ready")
		framework.ExpectNoError(
			waitForTestPodReady(f.ClientSet, 5*time.Minute, ns, "spdkcsi-test-xfs"),
			"wait for XFS test pod",
		)

		ginkgo.By("verify the mounted filesystem is XFS")
		// Alpine's df supports -T; XFS mounts report type 'xfs'.
		fsType, _ := execCommandInPod(f,
			"df -T /spdkvol | awk 'NR==2 {print $2}'",
			ns, &xfsPodLabel)
		gomega.Expect(fsType).To(gomega.ContainSubstring("xfs"),
			"filesystem type should be xfs")

		ginkgo.By("write data to the XFS volume")
		writeDataToPod(f, ns, &xfsPodLabel, "xfs-persistence-test-data", "/spdkvol/test")

		ginkgo.By("delete and recreate pod to verify persistence")
		deletePodByName(f.ClientSet, ns, "spdkcsi-test-xfs")
		framework.ExpectNoError(
			waitForTestPodGone(f.ClientSet, ns, "spdkcsi-test-xfs"),
			"wait for XFS pod to terminate",
		)
		framework.ExpectNoError(
			createPodForPVC(f.ClientSet, ns, "spdkcsi-test-xfs", "spdkcsi-pvc-xfs"),
			"recreate XFS test pod",
		)
		framework.ExpectNoError(
			waitForTestPodReady(f.ClientSet, 5*time.Minute, ns, "spdkcsi-test-xfs"),
			"wait for XFS pod after restart",
		)

		ginkgo.By("verify data survived the pod restart")
		compareDataInPod(f, ns, &xfsPodLabel,
			[]string{"xfs-persistence-test-data"},
			[]string{"/spdkvol/test"},
		)
	})

	// -------------------------------------------------------------------------
	// ext4 filesystem (default): subdirectory writes survive pod restart
	// -------------------------------------------------------------------------

	ginkgo.It("ext4 volume supports nested directory writes that persist across pod restarts", func() {
		ns := f.Namespace.Name
		testPodLabel := metav1.ListOptions{LabelSelector: "app=spdkcsi-pvc"}

		ginkgo.By("create PVC and test pod")
		deployPVC(ns)
		deployTestPod(ns)
		ginkgo.DeferCleanup(func() { deletePVCAndTestPod(ns) })

		ginkgo.By("wait for test pod to be ready")
		framework.ExpectNoError(
			waitForTestPodReady(f.ClientSet, 5*time.Minute, ns, testPodName),
			"wait for test pod",
		)

		ginkgo.By("create a subdirectory and write data")
		execCommandInPod(f, "mkdir -p /spdkvol/subdir/nested", ns, &testPodLabel)
		writeDataToPod(f, ns, &testPodLabel, "nested-dir-data", "/spdkvol/subdir/nested/file")

		ginkgo.By("delete and recreate pod")
		deleteTestPod(ns)
		framework.ExpectNoError(
			waitForTestPodGone(f.ClientSet, ns, testPodName),
			"wait for test pod to terminate",
		)
		deployTestPod(ns)
		framework.ExpectNoError(
			waitForTestPodReady(f.ClientSet, 5*time.Minute, ns, testPodName),
			"wait for test pod after restart",
		)

		ginkgo.By("verify nested directory and data survived the restart")
		compareDataInPod(f, ns, &testPodLabel,
			[]string{"nested-dir-data"},
			[]string{"/spdkvol/subdir/nested/file"},
		)
	})

	// -------------------------------------------------------------------------
	// Namespaced volumes: member pod restarts after master is deleted
	//
	// Regression test for the bug where initiator.Connect() extracted the
	// master's UUID from the NQN and called /volumes/<master-uuid>/connect.
	// When the master volume was deleted, this returned 404 and the member
	// pod was stuck in ContainerCreating indefinitely.
	// -------------------------------------------------------------------------

	ginkgo.It("namespaced volume member pod restarts successfully after master volume is deleted", func() {
		ns := f.Namespace.Name
		const nsidSC = "spdkcsi-e2e-nsid"

		ginkgo.By("create StorageClass with max_namespace_per_subsys=3")
		createStorageClassWithParams(f.ClientSet, nsidSC, map[string]string{
			"max_namespace_per_subsys": "3",
		})
		ginkgo.DeferCleanup(func() { deleteStorageClass(f.ClientSet, nsidSC) })

		const (
			pvc1Name = "spdkcsi-nsid-pvc1"
			pvc2Name = "spdkcsi-nsid-pvc2"
			pvc3Name = "spdkcsi-nsid-pvc3"
			pod1Name = "spdkcsi-nsid-pod1"
			pod2Name = "spdkcsi-nsid-pod2"
			pod3Name = "spdkcsi-nsid-pod3"
		)

		ginkgo.By("create three PVCs")
		for _, pvcName := range []string{pvc1Name, pvc2Name, pvc3Name} {
			framework.ExpectNoError(
				createPVC(f.ClientSet, ns, pvcName, nsidSC, 1<<30),
				"create PVC %s", pvcName,
			)
		}
		ginkgo.DeferCleanup(func() {
			for _, pvcName := range []string{pvc1Name, pvc2Name, pvc3Name} {
				_ = f.ClientSet.CoreV1().PersistentVolumeClaims(ns).Delete(
					context.Background(), pvcName, metav1.DeleteOptions{})
			}
		})

		ginkgo.By("create pods for all three PVCs")
		for _, pair := range [][2]string{{pod1Name, pvc1Name}, {pod2Name, pvc2Name}, {pod3Name, pvc3Name}} {
			framework.ExpectNoError(
				createPodForPVC(f.ClientSet, ns, pair[0], pair[1]),
				"create pod %s", pair[0],
			)
		}
		ginkgo.DeferCleanup(func() {
			for _, podName := range []string{pod1Name, pod2Name, pod3Name} {
				deletePodByName(f.ClientSet, ns, podName)
			}
		})

		ginkgo.By("wait for all pods to be ready")
		for _, podName := range []string{pod1Name, pod2Name, pod3Name} {
			framework.ExpectNoError(
				waitForTestPodReady(f.ClientSet, 5*time.Minute, ns, podName),
				"wait for pod %s", podName,
			)
		}

		// Read PV attributes to identify which volume is the master (its own UUID
		// appears in the NQN) and which are members (they share the master's NQN).
		type nsidPVC struct {
			pvcName string
			podName string
			attrs   pvVolumeAttrs
		}
		pvcPods := map[string]string{
			pvc1Name: pod1Name,
			pvc2Name: pod2Name,
			pvc3Name: pod3Name,
		}
		var all []nsidPVC
		for _, pvcName := range []string{pvc1Name, pvc2Name, pvc3Name} {
			attrs, err := getPVVolumeAttrs(f.ClientSet, ns, pvcName)
			framework.ExpectNoError(err, "get PV attrs for %s", pvcName)
			all = append(all, nsidPVC{pvcName: pvcName, podName: pvcPods[pvcName], attrs: attrs})
		}

		var master, keepMember, otherMember nsidPVC
		for _, p := range all {
			if strings.HasSuffix(p.attrs.NQN, ":lvol:"+p.attrs.VolName) {
				if master.pvcName == "" {
					master = p
				}
			} else {
				if keepMember.pvcName == "" {
					keepMember = p
				} else if otherMember.pvcName == "" {
					otherMember = p
				}
			}
		}
		if master.pvcName == "" || keepMember.pvcName == "" || otherMember.pvcName == "" {
			ginkgo.Skip("volumes did not share an NVMe subsystem (likely on different storage nodes) — namespaced volume test requires all three volumes on the same node")
			return
		}
		framework.Logf("master=%s (UUID %s), keepMember=%s, otherMember=%s",
			master.pvcName, master.attrs.VolName, keepMember.pvcName, otherMember.pvcName)

		ginkgo.By("write test data to the member volume we will keep")
		memberLabel := metav1.ListOptions{LabelSelector: "app=" + keepMember.podName}
		writeDataToPod(f, ns, &memberLabel, "nsid-persistence-data", "/spdkvol/test")

		// Delete master pod+PVC: removes the master UUID from sbcli so that any
		// subsequent /connect call using the master UUID returns 404.
		ginkgo.By("delete master pod and PVC")
		deletePodByName(f.ClientSet, ns, master.podName)
		framework.ExpectNoError(
			waitForTestPodGone(f.ClientSet, ns, master.podName),
			"wait for master pod gone",
		)
		framework.ExpectNoError(
			f.ClientSet.CoreV1().PersistentVolumeClaims(ns).Delete(
				context.Background(), master.pvcName, metav1.DeleteOptions{}),
			"delete master PVC",
		)
		framework.ExpectNoError(
			waitForPVDeleted(f.ClientSet, master.attrs.PVName, 5*time.Minute),
			"wait for master PV gone (sbcli must remove namespace before next step)",
		)

		// Delete the second member pod+PVC: removes its NVMe namespace device from
		// the node so that keepMember becomes the last device on the NQN.
		ginkgo.By("delete second member pod and PVC")
		deletePodByName(f.ClientSet, ns, otherMember.podName)
		framework.ExpectNoError(
			waitForTestPodGone(f.ClientSet, ns, otherMember.podName),
			"wait for other member pod gone",
		)
		framework.ExpectNoError(
			f.ClientSet.CoreV1().PersistentVolumeClaims(ns).Delete(
				context.Background(), otherMember.pvcName, metav1.DeleteOptions{}),
			"delete other member PVC",
		)
		framework.ExpectNoError(
			waitForPVDeleted(f.ClientSet, otherMember.attrs.PVName, 5*time.Minute),
			"wait for other member PV gone",
		)

		// Delete keepMember's pod: Disconnect() now finds exactly one NVMe device
		// for this NQN and performs an actual nvme disconnect, clearing the connection.
		ginkgo.By("delete member pod (last device on NQN — triggers real NVMe disconnect)")
		deletePodByName(f.ClientSet, ns, keepMember.podName)
		framework.ExpectNoError(
			waitForTestPodGone(f.ClientSet, ns, keepMember.podName),
			"wait for member pod gone",
		)

		// Recreate the pod: NodeStageVolume must now reconnect from scratch.
		// The fix uses volumeContext["name"] (member UUID) for /connect; without
		// the fix it used getLvolIDFromNQN (master UUID) which returns 404.
		ginkgo.By("recreate member pod and verify it becomes Ready")
		framework.ExpectNoError(
			createPodForPVC(f.ClientSet, ns, keepMember.podName, keepMember.pvcName),
			"recreate member pod",
		)
		framework.ExpectNoError(
			waitForTestPodReady(f.ClientSet, 5*time.Minute, ns, keepMember.podName),
			"member pod must reach Running (regression: 404 fetching connection via master UUID)",
		)

		ginkgo.By("verify data persisted through the pod restart")
		compareDataInPod(f, ns, &memberLabel,
			[]string{"nsid-persistence-data"},
			[]string{"/spdkvol/test"},
		)
	})
})
