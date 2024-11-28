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
			randomValue := r.Intn(n)

			ginkgo.By("check pvc write, clone, snapshot before node restart", func() {

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
				//write data to pvc and check
				deployPVC()
				deployTestPod()

				err = waitForTestPodReady(f.ClientSet, 3*time.Minute)
				if err != nil {
					ginkgo.Fail(err.Error())
				}
				writeDataToPod(f, &testPodLabel, persistData[0], persistDataPath[0])

				deleteTestPod()

				// snapshot and check
				deploySnapshot()

				err = waitForTestPodReady(f.ClientSet, 3*time.Minute)
				if err != nil {
					ginkgo.Fail(err.Error())
				}
				err = compareDataInPod(f, &testPodLabel, persistData, persistDataPath)
				if err != nil {
					ginkgo.Fail(err.Error())
				}

				// deleteSnapshot()

				// clone and check
				deployClone()

				err = waitForTestPodReady(f.ClientSet, 3*time.Minute)
				if err != nil {
					ginkgo.Fail(err.Error())
				}
				err = compareDataInPod(f, &testPodLabel, persistData, persistDataPath)
				if err != nil {
					ginkgo.Fail(err.Error())
				}
				// deleteClone()

			})

			ginkgo.By("restarting the storage node", func() {

				pvc, _ := c.CoreV1().PersistentVolumeClaims(nameSpace).Get(context.TODO(), "spdkcsi-pvc", metav1.GetOptions{})
				storageNodeID := pvc.Annotations["simplybk/host-id"]
				fmt.Println()
				err := restartStorageNode(c, storageNodeID)
				if err != nil {
					ginkgo.Fail(err.Error())
				}
				deleteClone()
				deleteSnapshot()
				deletePVC()

			})

			ginkgo.By("check pvc write, clone, snapshot after node restart", func() {
				deployPVC()
				deployTestPod()

				err := waitForTestPodReady(f.ClientSet, 3*time.Minute)
				if err != nil {
					ginkgo.Fail(err.Error())
				}
				writeDataToPod(f, &testPodLabel, persistData[0], persistDataPath[0])

				deleteTestPod()

				// check snapshot
				deploySnapshot()

				err = waitForTestPodReady(f.ClientSet, 3*time.Minute)
				if err != nil {
					ginkgo.Fail(err.Error())
				}
				err = compareDataInPod(f, &testPodLabel, persistData, persistDataPath)
				if err != nil {
					ginkgo.Fail(err.Error())
				}

				deleteSnapshot()

				//check clone

				deployClone()

				err = waitForTestPodReady(f.ClientSet, 3*time.Minute)
				if err != nil {
					ginkgo.Fail(err.Error())
				}
				err = compareDataInPod(f, &testPodLabel, persistData, persistDataPath)
				if err != nil {
					ginkgo.Fail(err.Error())
				}

				deleteClone()
				deletePVC()
			})
		})
	})
})
