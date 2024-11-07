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

var _ = ginkgo.Describe("SPDKCSI-CompleteWorkflow", func() {
	f := framework.NewDefaultFramework("spdkcsi")

	ginkgo.Context("Complete PVC and Snapshot Workflow", func() {
		ginkgo.It("Create PVC, Snapshot, and Clone with Node Restart", func() {
			var storageNodeID string
			testPodLabel := metav1.ListOptions{
				LabelSelector: "app=spdkcsi-pvc",
			}
			persistData := []string{"Data that needs to be stored"}
			persistDataPath := []string{"/spdkvol/test"}

			ginkgo.By("creating PVC and writing data")
			deployPVC()
			deployTestPod()
			defer deleteTestPod()

			err := waitForTestPodReady(f.ClientSet, 3*time.Minute)
			if err != nil {
				ginkgo.Fail(err.Error())
			}

			writeDataToPod(f, &testPodLabel, persistData[0], persistDataPath[0])

			ginkgo.By("retrieving storage-node-id from PVC annotations")
			pvc, err := f.ClientSet.CoreV1().PersistentVolumeClaims("spdk-csi").Get(context.TODO(), "spdkcsi-pvc", metav1.GetOptions{})
			if err != nil {
				ginkgo.Fail(err.Error())
			}
			storageNodeID = pvc.Annotations["simplybk/host-id"]

			ginkgo.By("restarting the storage node")
			err = restartStorageNode(storageNodeID)
			if err != nil {
				ginkgo.Fail(err.Error())
			}

			ginkgo.By("polling storage node status")
			err = pollStorageNodeStatus(storageNodeID, 20)
			if err != nil {
				ginkgo.Fail(err.Error())
			}

			ginkgo.By("creating snapshot and checking data")
			deploySnapshot()
			defer deleteSnapshot()

			err = waitForTestPodReady(f.ClientSet, 3*time.Minute)
			if err != nil {
				ginkgo.Fail(err.Error())
			}
			err = compareDataInPod(f, &testPodLabel, persistData, persistDataPath)
			if err != nil {
				ginkgo.Fail(err.Error())
			}

			ginkgo.By("creating clone and checking data")
			deployClone()
			defer deleteClone()

			err = waitForTestPodReady(f.ClientSet, 3*time.Minute)
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

func restartStorageNode(nodeID string) error {

	url := fmt.Sprintf("/storagenode/restart/%s", nodeID)

	_, err := rpcClient.CallSBCLI("GET", url, nil)
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
