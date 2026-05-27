package e2e

import (
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/test/e2e/framework"
)

var _ = ginkgo.Describe("SPDKCSI-NVMEOF", func() {
	f := framework.NewDefaultFramework("spdkcsi")

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
			ginkgo.By("create PVC and test pod")
			deployPVC()
			deployTestPod()
			// DeferCleanup is scoped to this It block and runs even on failure.
			ginkgo.DeferCleanup(deletePVCAndTestPod)

			ginkgo.By("wait for test pod to be ready")
			framework.ExpectNoError(
				waitForTestPodReady(f.ClientSet, 5*time.Minute, testPodName),
				"wait for test pod",
			)
		})

		ginkgo.It("filesystem volume can be expanded online", func() {
			pvcName := "spdkcsi-pvc"
			expandedSize := resource.MustParse("2Gi")
			testPodLabel := metav1.ListOptions{LabelSelector: "app=spdkcsi-pvc"}

			ginkgo.By("create PVC and test pod")
			deployPVC()
			deployTestPod()
			ginkgo.DeferCleanup(deletePVCAndTestPod)

			ginkgo.By("wait for test pod to be ready")
			framework.ExpectNoError(
				waitForTestPodReady(f.ClientSet, 5*time.Minute, testPodName),
				"wait for test pod",
			)

			ginkgo.By("resize PVC to 2Gi")
			framework.ExpectNoError(resizePVC(f.ClientSet, pvcName, expandedSize), "resize PVC")

			ginkgo.By("wait for PVC status capacity to reflect new size")
			framework.ExpectNoError(
				waitForPVCStorageCapacity(f.ClientSet, pvcName, expandedSize, 5*time.Minute),
				"wait for PVC capacity",
			)

			ginkgo.By("wait for filesystem inside pod to reflect new size")
			framework.ExpectNoError(
				waitForFilesystemSize(f, &testPodLabel, "/spdkvol", expandedSize.Value()*9/10, 5*time.Minute),
				"wait for filesystem resize",
			)
		})

		ginkgo.It("kubelet reports volume stats for a mounted volume", func() {
			testPodLabel := metav1.ListOptions{LabelSelector: "app=spdkcsi-pvc"}

			ginkgo.By("create PVC and test pod")
			deployPVC()
			deployTestPod()
			ginkgo.DeferCleanup(deletePVCAndTestPod)

			ginkgo.By("wait for test pod to be ready")
			framework.ExpectNoError(
				waitForTestPodReady(f.ClientSet, 5*time.Minute, testPodName),
				"wait for test pod",
			)

			ginkgo.By("write data so the volume is non-empty")
			writeDataToPod(f, &testPodLabel, "volume stats test data", "/spdkvol/stats-test")

			ginkgo.By("wait for kubelet to populate volume stats")
			framework.ExpectNoError(
				waitForMountedVolumeStats(f.ClientSet, testPodName, 5*time.Minute),
				"wait for kubelet volume stats",
			)
		})

		ginkgo.It("data persists across pod restarts when using multiple PVCs", func() {
			ginkgo.By("create three PVCs and a pod that mounts all three")
			deployMultiPvcs()
			deployTestPodWithMultiPvcs()
			ginkgo.DeferCleanup(func() {
				deleteMultiPvcsAndTestPodWithMultiPvcs()
				// Wait for full teardown so the next test suite starts clean.
				if err := waitForTestPodGone(f.ClientSet, multiTestPodName); err != nil {
					framework.Logf("timed out waiting for multi-PVC pod to terminate: %v", err)
				}
				for _, pvcName := range []string{"spdkcsi-pvc1", "spdkcsi-pvc2", "spdkcsi-pvc3"} {
					if err := waitForPvcGone(f.ClientSet, pvcName); err != nil {
						framework.Logf("timed out waiting for PVC %s to be deleted: %v", pvcName, err)
					}
				}
			})

			ginkgo.By("wait for multi-PVC pod to be ready")
			framework.ExpectNoError(
				waitForTestPodReady(f.ClientSet, 3*time.Minute, multiTestPodName),
				"wait for multi-PVC test pod",
			)

			ginkgo.By("verify data persists across a pod restart")
			checkDataPersistForMultiPvcs(f)
		})
	})
})
