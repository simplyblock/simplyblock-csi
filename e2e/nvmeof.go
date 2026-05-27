package e2e

import (
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/test/e2e/framework"
)

var _ = ginkgo.Describe("SPDKCSI-NVMEOF", func() {
	f := newTestFramework("spdkcsi")

	ginkgo.Context("Test SPDK CSI Dynamic Volume Provisioning", func() {

		ginkgo.It("CSI driver components should be running", func() {
			ginkgo.By("check controller StatefulSet is ready")
			framework.ExpectNoError(
				waitForControllerReady(f.ClientSet, 4*time.Minute),
				"wait for controller StatefulSet",
			)

			ginkgo.By("check node DaemonSet is ready")
			framework.ExpectNoError(
				waitForNodeServerReady(f.ClientSet, 3*time.Minute),
				"wait for node DaemonSet",
			)
		})

		ginkgo.It("dynamically provisioned volume binds and pod reaches Running", func() {
			ns := f.Namespace.Name

			ginkgo.By("create PVC and test pod")
			deployPVC(ns)
			deployTestPod(ns)
			// DeferCleanup is scoped to this It block and runs even on failure.
			ginkgo.DeferCleanup(func() { deletePVCAndTestPod(ns) })

			ginkgo.By("wait for test pod to be ready")
			framework.ExpectNoError(
				waitForTestPodReady(f.ClientSet, 5*time.Minute, ns, testPodName),
				"wait for test pod",
			)
		})

		ginkgo.It("filesystem volume can be expanded online", func() {
			ns := f.Namespace.Name
			pvcName := "spdkcsi-pvc"
			expandedSize := resource.MustParse("2Gi")
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

			ginkgo.By("resize PVC to 2Gi")
			framework.ExpectNoError(resizePVC(f.ClientSet, ns, pvcName, expandedSize), "resize PVC")

			ginkgo.By("wait for PVC status capacity to reflect new size")
			framework.ExpectNoError(
				waitForPVCStorageCapacity(f.ClientSet, ns, pvcName, expandedSize, 5*time.Minute),
				"wait for PVC capacity",
			)

			ginkgo.By("wait for filesystem inside pod to reflect new size")
			framework.ExpectNoError(
				waitForFilesystemSize(f, ns, &testPodLabel, "/spdkvol", expandedSize.Value()*9/10, 5*time.Minute),
				"wait for filesystem resize",
			)
		})

		ginkgo.It("kubelet reports volume stats for a mounted volume", func() {
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

			ginkgo.By("write data so the volume is non-empty")
			writeDataToPod(f, ns, &testPodLabel, "volume stats test data", "/spdkvol/stats-test")

			ginkgo.By("wait for kubelet to populate volume stats")
			framework.ExpectNoError(
				waitForMountedVolumeStats(f.ClientSet, ns, testPodName, 5*time.Minute),
				"wait for kubelet volume stats",
			)
		})

		ginkgo.It("data persists across pod restarts when using multiple PVCs", func() {
			ns := f.Namespace.Name

			ginkgo.By("create three PVCs and a pod that mounts all three")
			deployMultiPvcs(ns)
			deployTestPodWithMultiPvcs(ns)
			ginkgo.DeferCleanup(func() {
				deleteMultiPvcsAndTestPodWithMultiPvcs(ns)
				// Wait for full teardown so the next test suite starts clean.
				if err := waitForTestPodGone(f.ClientSet, ns, multiTestPodName); err != nil {
					framework.Logf("timed out waiting for multi-PVC pod to terminate: %v", err)
				}
				for _, pvcName := range []string{"spdkcsi-pvc1", "spdkcsi-pvc2", "spdkcsi-pvc3"} {
					if err := waitForPvcGone(f.ClientSet, ns, pvcName); err != nil {
						framework.Logf("timed out waiting for PVC %s to be deleted: %v", pvcName, err)
					}
				}
			})

			ginkgo.By("wait for multi-PVC pod to be ready")
			framework.ExpectNoError(
				waitForTestPodReady(f.ClientSet, 3*time.Minute, ns, multiTestPodName),
				"wait for multi-PVC test pod",
			)

			ginkgo.By("verify data persists across a pod restart")
			checkDataPersistForMultiPvcs(f, ns)
		})
	})
})
