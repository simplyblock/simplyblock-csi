package e2e

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	ginko "github.com/onsi/ginkgo/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
)

// This test verifies that the external health monitor controller surfaces PVC events
// when the CSI driver's ControllerGetVolume returns an abnormal condition.
// It leverages a temporary storage-node restart to cause a brief outage so that
// the health monitor observes Abnormal=true for the affected volume.
var _ = ginko.Describe("Volume Health Monitoring", func() {
	f := framework.NewDefaultFramework("spdkcsi-health")

	ginko.It("surfaces abnormal condition event on PVC when volume becomes unavailable", func() {
		c := f.ClientSet

		// Pick a storage node where we'll schedule the volume
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		n, err := numberOfNodes(c)
		framework.ExpectNoError(err, "fetch number of storage nodes")
		framework.ExpectNotEqual(n, 0, "expected at least one storage node")
		randomValue := r.Intn(n)
		hostID, _, err := getStorageNode(c, randomValue)
		framework.ExpectNoError(err, "get a storage node")

		// Unique names per test run
		storageClassName := fmt.Sprintf("spdk-health-sc-%d", time.Now().UnixNano())
		pvcName := fmt.Sprintf("spdk-health-pvc-%d", time.Now().UnixNano())
		podName := fmt.Sprintf("spdk-health-pod-%d", time.Now().UnixNano())

		// Create SC -> PVC -> Pod
		ginko.By("creating StorageClass bound to chosen storage node")
		err = createstorageClassWithHostID(c, storageClassName, hostID)
		framework.ExpectNoError(err, "create storageclass with hostID")

		defer func() {
			_ = c.StorageV1().StorageClasses().Delete(context.Background(), storageClassName, metav1.DeleteOptions{})
		}()

		ginko.By("creating PVC")
		err = createPVC(c, nameSpace, pvcName, storageClassName, 1*1024*1024*1024)
		framework.ExpectNoError(err, "create PVC")

		defer func() {
			_ = c.CoreV1().PersistentVolumeClaims(nameSpace).Delete(context.Background(), pvcName, metav1.DeleteOptions{})
		}()

		ginko.By("creating Pod that uses the PVC")
		err = createSimplePod(c, nameSpace, podName, pvcName)
		framework.ExpectNoError(err, "create pod")

		defer func() {
			_ = c.CoreV1().Pods(nameSpace).Delete(context.Background(), podName, metav1.DeleteOptions{})
		}()

		// Instead of touching storage node, simulate client-side failure by blocking
        // controller egress to Simplyblock API via NetworkPolicy.
        ginko.By("blocking controller egress to Simplyblock API to induce health event")
        apiIP, err := getSimplyblockAPIIP(c)
        framework.ExpectNoError(err, "get Simplyblock API IP")
        npName := fmt.Sprintf("block-sb-api-%d", time.Now().UnixNano())
        err = createControllerEgressBlockPolicy(c, nameSpace, npName, apiIP)
        framework.ExpectNoError(err, "create NetworkPolicy to block API")
        defer func() { _ = deleteNetworkPolicy(c, nameSpace, npName) }()

        ginko.By("waiting for PVC abnormal health event while API is blocked")
        err = waitForPVCHealthEventContains(c, nameSpace, pvcName, []string{"abnormal", "unhealthy", "volume condition"}, 5*time.Minute)
        framework.ExpectNoError(err, "expect PVC abnormal health event")
	})
})

// waitForPVCHealthEventContains polls Events for the PVC and returns success if any event
// message contains one of the provided substrings (case-insensitive) before timeout.
func waitForPVCHealthEventContains(c kubernetes.Interface, ns, pvcName string, substrs []string, timeout time.Duration) error {
	ctxTimeout, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	fieldSel := fields.AndSelectors(
		fields.OneTermEqualSelector("involvedObject.kind", "PersistentVolumeClaim"),
		fields.OneTermEqualSelector("involvedObject.name", pvcName),
	)

	for {
		select {
		case <-ctxTimeout.Done():
			return fmt.Errorf("timed out waiting for PVC %s health event", pvcName)
		case <-ticker.C:
			list, err := c.CoreV1().Events(ns).List(ctxTimeout, metav1.ListOptions{FieldSelector: fieldSel.String()})
			if err != nil {
				return fmt.Errorf("failed to list events for PVC %s: %w", pvcName, err)
			}
			for _, ev := range list.Items {
				msg := strings.ToLower(ev.Message)
				for _, sub := range substrs {
					if strings.Contains(msg, strings.ToLower(sub)) {
						return nil
					}
				}
			}
		}
	}
}
