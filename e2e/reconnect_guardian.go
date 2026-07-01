package e2e

import (
	"context"
	"fmt"
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

var _ = ginkgo.Describe("SPDKCSI-RECONNECT-GUARDIAN", func() {
	f := newTestFramework("spdkcsi")

	ginkgo.Context("guardian auto-restarts a workload after total NVMe-oF path loss", func() {
		// Companion to SPDKCSI-RECONNECT-FULLLOSS. That test force-deletes the pod
		// itself (standing in for the guardian) to verify the restage-on-publish
		// path fast and deterministically. This test verifies the guardian's own
		// job: after total path loss it must, on its poll cycle, detect the broken
		// lvol and restart the opted-in pod WITHOUT anyone else deleting it. That
		// matters because an idle workload container never crashes on volume I/O
		// errors, so in production nothing but the guardian would restart it.
		//
		// We induce total path loss and then do NOT touch the pod, so the only
		// thing that can produce a new pod is the guardian. We assert a replacement
		// pod (new UID) comes up on its own and the volume is usable again. Opt-in
		// is via the StorageClass label simplyblock.io/auto-restart-on-pathloss.
		//
		// This is intentionally slow: it waits for the guardian poll cycle
		// (default 5m) plus the restart and restage.
		ginkgo.It("restarts an opted-in pod and restages its volume after total path loss", func() {
			m := fullLossMode{name: "ext4 filesystem", fsType: "ext4"}
			w := setupManagedWorkload(f, m, "guardian")
			origUID := string(w.pod.UID)
			w.induceTotalPathLoss(f)

			ginkgo.By("do NOT touch the pod; wait for the guardian to restart it and restage the volume")
			// The workload is an idle `sleep`, so it never restarts on its own — only
			// the guardian will replace it. A new-UID Ready pod therefore proves the
			// guardian acted; we also confirm the volume is usable again on it. Allow
			// a full guardian poll cycle (default 5m) plus the restart and restage.
			token := "recovered-" + w.ns
			gomega.Eventually(func() error {
				if !guardianReplacedPod(f.ClientSet, w.ns, w.appLabel, origUID) {
					return fmt.Errorf("guardian has not restarted the pod yet (still UID %s)", origUID)
				}
				return verifyVolumeUsableE(f, w.ns, w.appLabel, m, token)
			}, 12*time.Minute, 10*time.Second).Should(gomega.Succeed(),
				"guardian did not restart the pod and restage the volume after total path loss")
		})
	})
})

// guardianReplacedPod reports whether a Running+Ready pod matching app=appLabel
// exists whose UID differs from origUID — i.e. the original pod was replaced
// without this test deleting it, which only the guardian does.
func guardianReplacedPod(c kubernetes.Interface, ns, appLabel, origUID string) bool {
	pods, err := c.CoreV1().Pods(ns).List(context.Background(), metav1.ListOptions{LabelSelector: "app=" + appLabel})
	if err != nil {
		return false
	}
	for i := range pods.Items {
		p := &pods.Items[i]
		if string(p.UID) != origUID && p.DeletionTimestamp == nil &&
			p.Status.Phase == corev1.PodRunning && podReady(p) {
			return true
		}
	}
	return false
}
