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
		ginkgo.It("CSI driver components should function properly", func() {
			ginkgo.By("checking controller statefulset is running", func() {
				err := waitForControllerReady(f.ClientSet, 4*time.Minute)
				if err != nil {
					ginkgo.Fail(err.Error())
				}
			})

			ginkgo.By("checking node daemonset is running", func() {
				err := waitForNodeServerReady(f.ClientSet, 3*time.Minute)
				if err != nil {
					ginkgo.Fail(err.Error())
				}
			})
		})

		ginkgo.It("Test the flow for Dynamic volume provisioning", func() {
			testPodName := "spdkcsi-test"
			// WaitForFirstConsumer StorageClasses defer provisioning until a pod is scheduled.
			// Deploy a pod alongside the PVC so the CSI provisioner is triggered, then verify
			// the PV is created and the pod reaches Running before testing persistence.
			ginkgo.By("creating a PVC and binding it to a pod", func() {
				deployPVC()
				deployTestPod()
				defer deletePVCAndTestPod()
				err := waitForTestPodReady(f.ClientSet, 5*time.Minute, testPodName)
				if err != nil {
					ginkgo.Fail(err.Error())
				}
			})
		})

		ginkgo.It("Test filesystem volume expansion", func() {
			testPodName := "spdkcsi-test"
			pvcName := "spdkcsi-pvc"
			expandedSize := resource.MustParse("2Gi")
			testPodLabel := metav1.ListOptions{
				LabelSelector: "app=spdkcsi-pvc",
			}

			ginkgo.By("creating a PVC and pod", func() {
				deployPVC()
				deployTestPod()
				defer deletePVCAndTestPod()

				err := waitForTestPodReady(f.ClientSet, 5*time.Minute, testPodName)
				if err != nil {
					ginkgo.Fail(err.Error())
				}

				err = resizePVC(f.ClientSet, pvcName, expandedSize)
				if err != nil {
					ginkgo.Fail(err.Error())
				}

				err = waitForPVCStorageCapacity(f.ClientSet, pvcName, expandedSize, 5*time.Minute)
				if err != nil {
					ginkgo.Fail(err.Error())
				}

				err = waitForFilesystemSize(f, &testPodLabel, "/spdkvol", expandedSize.Value()*9/10, 5*time.Minute)
				if err != nil {
					ginkgo.Fail(err.Error())
				}
			})
		})

		ginkgo.It("Test mounted volume stats", func() {
			testPodName := "spdkcsi-test"
			testPodLabel := metav1.ListOptions{
				LabelSelector: "app=spdkcsi-pvc",
			}

			ginkgo.By("creating a mounted PVC with data", func() {
				deployPVC()
				deployTestPod()
				defer deletePVCAndTestPod()

				err := waitForTestPodReady(f.ClientSet, 5*time.Minute, testPodName)
				if err != nil {
					ginkgo.Fail(err.Error())
				}

				writeDataToPod(f, &testPodLabel, "volume stats test data", "/spdkvol/stats-test")

				err = waitForMountedVolumeStats(f.ClientSet, testPodName, 5*time.Minute)
				if err != nil {
					ginkgo.Fail(err.Error())
				}
			})
		})

		ginkgo.It("Test multiple PVCs", func() {
			multiTestPodName := "spdkcsi-test-multi"
			ginkgo.By("create multiple pvcs and a pod with multiple pvcs attached, and check data persistence after the pod is removed and recreated", func() {
				deployMultiPvcs()
				deployTestPodWithMultiPvcs()
				defer func() {
					deleteMultiPvcsAndTestPodWithMultiPvcs()
					if err := waitForTestPodGone(f.ClientSet, multiTestPodName); err != nil {
						ginkgo.Fail(err.Error())
					}
					for _, pvcName := range []string{"spdkcsi-pvc1", "spdkcsi-pvc2", "spdkcsi-pvc3"} {
						if err := waitForPvcGone(f.ClientSet, pvcName); err != nil {
							ginkgo.Fail(err.Error())
						}
					}
				}()
				err := waitForTestPodReady(f.ClientSet, 3*time.Minute, multiTestPodName)
				if err != nil {
					ginkgo.Fail(err.Error())
				}

				err = checkDataPersistForMultiPvcs(f)
				if err != nil {
					ginkgo.Fail(err.Error())
				}
			})
		})
	})
})

var _ = ginkgo.Describe("CACHE-NODE-TEST", func() {
	f := framework.NewDefaultFramework("spdkcsi")

	ginkgo.Context("Test caching node SPDK CSI Dynamic Volume Provisioning", func() {

		ginkgo.It("Test the flow for Caching nodes", func() {
			testPodName := "spdkcsi-test"
			ginkgo.By("creating a caching PVC and bind it to a pod", func() {
				deployCachePVC()
				deployCacheTestPod()
				defer deleteCachePVCAndCacheTestPod()
				err := waitForCacheTestPodReady(f.ClientSet, 3*time.Minute)
				if err != nil {
					ginkgo.Fail(err.Error())
				}
			})

			ginkgo.By("check data persistency after the pod is removed and recreated", func() {
				deployPVC()
				deployTestPod()
				defer deletePVCAndTestPod()

				err := waitForTestPodReady(f.ClientSet, 3*time.Minute, testPodName)
				if err != nil {
					ginkgo.Fail(err.Error())
				}

				err = checkDataPersist(f)
				if err != nil {
					ginkgo.Fail(err.Error())
				}
			})
		})
	})
})
