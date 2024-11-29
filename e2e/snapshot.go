package e2e

import (
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/test/e2e/framework"
)

var _ = ginkgo.Describe("SPDKCSI-SNAPSHOT", func() {
	f := framework.NewDefaultFramework("spdkcsi")

	ginkgo.Context("Test SPDK CSI Snapshot", func() {
		ginkgo.It("Test SPDK CSI Snapshot", func() {
			testPodLabel := metav1.ListOptions{
				LabelSelector: "app=spdkcsi-pvc",
			}
			persistData := []string{"Data that needs to be stored"}
			persistDataPath := []string{"/spdkvol/test"}
			persistData2 := []string{"Data that needs to be stored", "Second data that needs to be stored"}
			persistDataPath2 := []string{"/spdkvol/test", "/spdkvol/test2"}

			ginkgo.By("create source pvc and write data", func() {
				deployPVC()
				deployTestPod()
				defer deleteTestPod()
				// do not delete pvc here, since we need it for snapshot

				err := waitForTestPodReady(f.ClientSet, 3*time.Minute, "spdkcsi-test")
				if err != nil {
					ginkgo.Fail(err.Error())
				}
				// write data to source pvc
				writeDataToPod(f, &testPodLabel, persistData[0], persistDataPath[0])
			})

			ginkgo.By("create snapshot1 and check data persistency", func() {
				deploySnapshot()
				defer deleteSnapshot()

				err := waitForTestPodReady(f.ClientSet, 3*time.Minute, "spdkcsi-test-snapshot1")
				if err != nil {
					ginkgo.Fail(err.Error())
				}
				err = compareDataInPod(f, &testPodLabel, persistData, persistDataPath)
				if err != nil {
					ginkgo.Fail(err.Error())
				}
			})

			ginkgo.By("write second data to the same PVC", func() {
				deployTestPod()
				defer deleteTestPod()

				err := waitForTestPodReady(f.ClientSet, 3*time.Minute, "spdkcsi-test")
				if err != nil {
					ginkgo.Fail(err.Error())
				}
				// write second data to source pvc
				writeDataToPod(f, &testPodLabel, persistData2[1], persistDataPath2[1])
			})

			ginkgo.By("create snapshot2 and check second data persistency", func() {
				deploySnapshot2()
				defer deleteSnapshot2()
				defer deletePVC()

				err := waitForTestPodReady(f.ClientSet, 3*time.Minute, "s")
				if err != nil {
					ginkgo.Fail(err.Error())
				}
				err = compareDataInPod(f, &testPodLabel, persistData2, persistDataPath2)
				if err != nil {
					ginkgo.Fail(err.Error())
				}
			})
		})
	})
})
