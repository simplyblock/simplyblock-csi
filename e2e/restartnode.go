package e2e

import (
	"context"
	"fmt"
	"os"
	"time"

	"math/rand"

	ginkgo "github.com/onsi/ginkgo/v2"
	"gopkg.in/yaml.v3"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/test/e2e/framework"
)

type storageNode struct {
	UUID   string `json:"uuid"`
	NodeIP string `json:"node_ip"`
}

var _ = ginkgo.Describe("SPDKCSI-NodeRestart", func() {
	f := framework.NewDefaultFramework("spdkcsi")

	ginkgo.Context("Test SPDK CSI node restart", func() {
		ginkgo.It("Test SPDK CSI node restart", func() {
			testPodLabel := metav1.ListOptions{
				LabelSelector: "app=spdkcsi-pvc",
			}
			persistData := []string{"Data that needs to be stored"}
			persistDataPath := []string{"/spdkvol/test"}
			c := f.ClientSet
			n, err := numberOfNodes(c)
			if err != nil {
				ginkgo.Fail(err.Error())
			}
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			randomValue := r.Intn(n - 1)

			ginkgo.By("create source pvc and write data", func() {

				storageNodeID, _ := getStorageNodeId(c, randomValue)

				data, _ := os.ReadFile(pvcPath)
				var yamlContent map[string]interface{}
				err := yaml.Unmarshal(data, &yamlContent)
				if err != nil {
					ginkgo.Fail(err.Error())
				}

				annotations, _ := yamlContent["metadata"].(map[string]interface{})["annotations"].(map[string]interface{})
				annotations["simplybk/host-id"] = storageNodeID
				updatedData, err := yaml.Marshal(yamlContent)
				if err != nil {
					ginkgo.Fail(err.Error())
				}

				err = os.WriteFile(pvcPath, updatedData, 0644)
				if err != nil {
					ginkgo.Fail(err.Error())
				}

				deployPVC()
				deployTestPod()
				defer deleteTestPod()

				err = waitForTestPodReady(f.ClientSet, 3*time.Minute)
				if err != nil {
					ginkgo.Fail(err.Error())
				}
				writeDataToPod(f, &testPodLabel, persistData[0], persistDataPath[0])
			})

			ginkgo.By("create snapshot and check data persistency", func() {
				deploySnapshot()
				defer deleteSnapshot()

				err := waitForTestPodReady(f.ClientSet, 3*time.Minute)
				if err != nil {
					ginkgo.Fail(err.Error())
				}
				err = compareDataInPod(f, &testPodLabel, persistData, persistDataPath)
				if err != nil {
					ginkgo.Fail(err.Error())
				}
			})

			ginkgo.By("create clone and check data persistency", func() {
				deployClone()
				defer deleteClone()

				err := waitForTestPodReady(f.ClientSet, 3*time.Minute)
				if err != nil {
					ginkgo.Fail(err.Error())
				}
				err = compareDataInPod(f, &testPodLabel, persistData, persistDataPath)
				if err != nil {
					ginkgo.Fail(err.Error())
				}
			})

			ginkgo.By("restarting the storage node", func() {

				pvc, _ := c.CoreV1().PersistentVolumeClaims(nameSpace).Get(context.TODO(), "spdkcsi-pvc", metav1.GetOptions{})
				storageNodeID := pvc.Annotations["simplybk/host-id"]
				fmt.Println()
				err := restartStorageNode(c, storageNodeID)
				if err != nil {
					ginkgo.Fail(err.Error())
				}
				defer deletePVC()

			})

			ginkgo.By("create source pvc and write data", func() {
				deployPVC()
				deployTestPod()
				defer deleteTestPod()

				err := waitForTestPodReady(f.ClientSet, 3*time.Minute)
				if err != nil {
					ginkgo.Fail(err.Error())
				}
				writeDataToPod(f, &testPodLabel, persistData[0], persistDataPath[0])
			})

			ginkgo.By("create snapshot and check data persistency", func() {
				deploySnapshot()
				defer deleteSnapshot()

				err := waitForTestPodReady(f.ClientSet, 3*time.Minute)
				if err != nil {
					ginkgo.Fail(err.Error())
				}
				err = compareDataInPod(f, &testPodLabel, persistData, persistDataPath)
				if err != nil {
					ginkgo.Fail(err.Error())
				}
			})

			ginkgo.By("create clone and check data persistency", func() {
				deployClone()
				defer deleteClone()
				defer deletePVC()

				err := waitForTestPodReady(f.ClientSet, 3*time.Minute)
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
