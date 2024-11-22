package e2e

import (
	"context"
	"net/http"
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/spdk/spdk-csi/pkg/util"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/test/e2e/framework"
)

// var (
// 	rpcClient util.RPCClient
// )

type storageNode struct {
	UUID   string `json:"uuid"`
	NodeIP string `json:"node_ip"`
}

type storageNodeResp struct {
	ApiEndpoint string `json:"api_endpoint"`
}

func (s SimplyBlock) initializeRPCClient() util.RPCClient {
	return util.RPCClient{
		ClusterID:     s.UUID,
		ClusterIP:     s.IP,
		ClusterSecret: s.Secret,
		HTTPClient:    &http.Client{Timeout: 10 * time.Second},
	}
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

			ginkgo.By("retrieving storage-node-id from PVC annotations", func() {
				pvc, err := f.ClientSet.CoreV1().PersistentVolumeClaims(nameSpace).Get(context.TODO(), "spdkcsi-pvc", metav1.GetOptions{})
				if err != nil {
					ginkgo.Fail(err.Error())
				}
				storageNodeID = pvc.Annotations["simplybk/host-id"]

				defer deletePVC()

			})

			ginkgo.By("restarting the storage node", func() {

				c := f.ClientSet
				err := restartStorageNode(c, storageNodeID)
				if err != nil {
					ginkgo.Fail(err.Error())
				}
			})

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
		})
	})
})
