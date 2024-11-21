package e2e

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/spdk/spdk-csi/pkg/util"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/test/e2e/framework"
)

var (
	rpcClient util.RPCClient
)


type storageNode struct {
    UUID   string `json:"uuid"`
    NodeIP string `json:"node_ip"`
}

type storageNodeResp struct{
	ApiEndpoint string `json:"api_endpoint"`
}


var _ = ginkgo.Describe("SPDKCSI-NodeRestart", func() {
	f := framework.NewDefaultFramework("spdkcsi")

	ginkgo.Context("Test SPDK CSI node restart", func() {
		ginkgo.It("Test SPDK CSI node restart", func() {
			var storageNodeID string
			testPodLabel := metav1.ListOptions{
				LabelSelector: "app=spdkcsi-pvc",
			}
			persistData := []string{"Data that needs to be stored"}
			persistDataPath := []string{"/spdkvol/test"}

			
			ginkgo.By("create source pvc and write data", func() {
				deployPVC()
				deployTestPod()
				defer deleteTestPod()
				// do not delete pvc here, since we need it for snapshot

				err := waitForTestPodReady(f.ClientSet, 3*time.Minute)
				if err != nil {
					ginkgo.Fail(err.Error())
				}
				// write data to source pvc
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
			
			

			


			ginkgo.By("retrieving storage-node-id from PVC annotations",func(){
				pvc, err := f.ClientSet.CoreV1().PersistentVolumeClaims("spdk-csi").Get(context.TODO(), "spdkcsi-pvc", metav1.GetOptions{})
				if err != nil {
					ginkgo.Fail(err.Error())
				}
				storageNodeID = pvc.Annotations["simplybk/host-id"]

				defer deletePVC()

			})


			ginkgo.By("restarting the storage node", func(){
				err = restartStorageNode(storageNodeID)
				if err != nil {
					ginkgo.Fail(err.Error())
				}
			})



			ginkgo.By("polling storage node status",func(){
				err = pollStorageNodeStatus(storageNodeID, 20)
				if err != nil {
					ginkgo.Fail(err.Error())
			}})


			ginkgo.By("create source pvc and write data", func() {
				deployPVC()
				deployTestPod()
				defer deleteTestPod()
				// do not delete pvc here, since we need it for snapshot

				err := waitForTestPodReady(f.ClientSet, 3*time.Minute)
				if err != nil {
					ginkgo.Fail(err.Error())
				}
				// write data to source pvc
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





			



func restartStorageNode(nodeID string) error {

	url := fmt.Sprintf("/storagenode/suspend/%s", nodeID)

	_, err := rpcClient.CallSBCLI("GET", url, nil)
	if err != nil {
		return errors.New("failed to suspend storage node: " + err.Error())
	}

	url := fmt.Sprintf("/storagenode/shutdown/%s/?force=True", nodeID)

	_, err := rpcClient.CallSBCLI("GET", url, nil)
	if err != nil {
		return errors.New("failed to shutdown storage node: " + err.Error())
	}
	

	url := fmt.Sprintf("/storagenode/%s",nodeID)
	resp,err := rpcClient.CallSBCLI("GET",url, nil)
	data := storageNodeResp{}
	json.Unmarshal(resp, data)




	args := Args{
		UUID: nodeID,
		NodeIP: data.api_endpoint,
	}

	url := fmt.Sprintf("/storagenode/restart/%s", nodeID, args)

	_, err := rpcClient.CallSBCLI("PUT", url, nil)
	if err != nil {
		return errors.New("failed to restart storage node: " + err.Error())
	}
	
	return nil
}




func pollStorageNodeStatus(nodeID string, timeout int) error {
	url := fmt.Sprintf("/storagenode/%s", nodeID)
	for i := 0; i < timeout; i++ {
		response, err := rpcClient.CallSBCLI("GET", url, nil)
		if err != nil {
			return err
		}

		responseData, ok := response.([]byte)
		if !ok {
			return errors.New("failed to assert response to []byte")
		}

		var nodeStatus map[string]interface{}
		err = json.Unmarshal(responseData, &nodeStatus)
		if err != nil {
			return err
		}

		if nodeStatus["status_code"] == "online" {
			return nil
		}

		time.Sleep(3 * time.Second)
	}
	return errors.New("storage node did not come online in time")
}



