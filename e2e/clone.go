package e2e

import (
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/test/e2e/framework"
)

var _ = ginkgo.Describe("SPDKCSI-CLONE", func() {
	f := framework.NewDefaultFramework("spdkcsi")

	ginkgo.Context("Test SPDK CSI Volume Clone", func() {
		ginkgo.It("Test SPDK CSI Clone", func() {
			testPodLabel := metav1.ListOptions{
				LabelSelector: "app=spdkcsi-pvc",
			}
			testPodName := "spdkcsi-test"
			cloneTestPodName := "spdkcsi-test-clone"
			persistData := []string{"Data that needs to be stored"}
			persistDataPath := []string{"/spdkvol/test"}

			ginkgo.By("create source pvc and write data", func() {
				deployPVC()
				deployTestPod()
				defer deleteTestPod()

				err := waitForTestPodReady(f.ClientSet, 3*time.Minute, testPodName)
				if err != nil {
					ginkgo.Fail(err.Error())
				}
				writeDataToPod(f, &testPodLabel, persistData[0], persistDataPath[0])
			})

			ginkgo.By("create clone and check data persistency", func() {
				deployClone()
				defer deleteClone()
				defer deletePVC()

				err := waitForTestPodReady(f.ClientSet, 3*time.Minute, cloneTestPodName)
				if err != nil {
					ginkgo.Fail(err.Error())
				}
				err = compareDataInPod(f, &testPodLabel, persistData, persistDataPath)
				if err != nil {
					ginkgo.Fail(err.Error())
				}
			})
		})
	})
})
