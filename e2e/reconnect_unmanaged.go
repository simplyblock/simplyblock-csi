package e2e

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
)

// uuidRe extracts a volume UUID from sbctl output.
var uuidRe = regexp.MustCompile(`[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}`)

var _ = ginkgo.Describe("SPDKCSI-RECONNECT-UNMANAGED", func() {
	f := newTestFramework("spdkcsi")

	ginkgo.Context("NVMe-oF path recovery is gated on PV/PVC management", func() {
		// A simplyblock volume created directly via the API (no PV/PVC) is still
		// an NVMe-oF subsystem on the host, but the node plugin must NOT manage
		// its paths. We connect such a volume, degrade it, and confirm the
		// monitor leaves it alone (no recovery) — the behaviour the positive
		// SPDKCSI-RECONNECT test shows it WOULD apply to a managed volume.
		ginkgo.It("skips a connected simplyblock volume that has no PV/PVC", func() {
			pool := envOr("E2E_SB_POOL", "testing1")
			size := envOr("E2E_SB_VOLUME_SIZE", "1G")
			volName := "e2e-unmanaged-" + f.Namespace.Name

			ginkgo.By("check driver components are running")
			framework.ExpectNoError(waitForNodeServerReady(f.ClientSet, 3*time.Minute), "node DaemonSet ready")

			ginkgo.By("pick a worker node and its csi-node pod")
			workerNode, pluginPod, pluginContainer := anyNodePluginPod(f.ClientSet)
			framework.Logf("using node %q (csi-node pod %q)", workerNode, pluginPod)

			ginkgo.By(fmt.Sprintf("create an unmanaged volume %q via sbctl", volName))
			addOut := sbctl(f, fmt.Sprintf("volume add %s %s %s", volName, size, pool))
			volID := uuidRe.FindString(addOut)
			gomega.Expect(volID).NotTo(gomega.BeEmpty(), "could not parse volume UUID from sbctl output: %q", addOut)
			framework.Logf("created unmanaged volume %s (id %s)", volName, volID)
			// Delete the backend volume even if later steps fail.
			ginkgo.DeferCleanup(func() {
				out := sbctl(f, "volume delete "+volID)
				framework.Logf("sbctl volume delete %s: %s", volID, out)
			})

			ginkgo.By("connect the volume on the chosen host")
			connectOut := sbctl(f, "volume connect "+volID)
			connectCmds := nvmeConnectCommands(connectOut)
			gomega.Expect(connectCmds).NotTo(gomega.BeEmpty(),
				"no `nvme connect` commands in sbctl output: %q", connectOut)
			for _, cmd := range connectCmds {
				execInPod(f, driverNamespace(), pluginPod, pluginContainer, cmd)
			}
			// Disconnect from the host even if later steps fail. Re-list at
			// cleanup time to resolve the subsystem NQN.
			ginkgo.DeferCleanup(func() {
				if s := subsystemForLvol(listSubsystems(f, pluginPod, pluginContainer), volID); s != nil {
					execInPod(f, driverNamespace(), pluginPod, pluginContainer,
						"nvme disconnect -n "+s.NQN+" || true")
				}
			})

			ginkgo.By("confirm the volume is connected as multipath")
			sub := waitForSubsystem(f, pluginPod, pluginContainer, volID, time.Minute)
			origLive := liveControllers(sub)
			framework.Logf("unmanaged volume %s subsystem %s has live paths: %v", volID, sub.NQN, origLive)
			if len(origLive) < 2 {
				ginkgo.Skip(fmt.Sprintf(
					"unmanaged volume has %d live path(s); a single path cannot be degraded "+
						"to observe (non-)recovery", len(origLive)))
			}

			ginkgo.By("drop one NVMe path by deleting its controller on the node")
			victim := origLive[len(origLive)-1]
			execInPod(f, driverNamespace(), pluginPod, pluginContainer,
				fmt.Sprintf("echo 1 > /sys/class/nvme/%s/delete_controller", victim))

			ginkgo.By("confirm the path count actually dropped")
			gomega.Eventually(func() int {
				return liveCount(f, pluginPod, pluginContainer, volID)
			}, 30*time.Second, 2*time.Second).Should(gomega.BeNumerically("<", len(origLive)),
				"expected a path to drop after deleting controller %s", victim)

			ginkgo.By("verify the node plugin does NOT recover the unmanaged volume")
			// A managed volume recovers within ~1 minute (see SPDKCSI-RECONNECT).
			// Hold for longer and assert the degraded path is never restored.
			gomega.Consistently(func() int {
				return liveCount(f, pluginPod, pluginContainer, volID)
			}, 90*time.Second, 5*time.Second).Should(gomega.BeNumerically("<", len(origLive)),
				"node plugin must not reconnect paths for a volume with no PV/PVC")
		})
	})
})

// envOr returns the env var value or a default.
func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// anyNodePluginPod returns an arbitrary csi-node DaemonSet pod, its node, and
// its first container.
func anyNodePluginPod(c kubernetes.Interface) (nodeName, podName, container string) {
	dns := driverNamespace()
	ds, err := c.AppsV1().DaemonSets(dns).Get(context.Background(), nodeDsName, metav1.GetOptions{})
	framework.ExpectNoError(err, "get node DaemonSet %s/%s", dns, nodeDsName)

	pods, err := c.CoreV1().Pods(dns).List(context.Background(), metav1.ListOptions{
		LabelSelector: labels.Set(ds.Spec.Selector.MatchLabels).String(),
	})
	framework.ExpectNoError(err, "list csi-node pods")
	gomega.Expect(pods.Items).NotTo(gomega.BeEmpty(), "no csi-node pods found")

	pod := pods.Items[0]
	return pod.Spec.NodeName, pod.Name, pod.Spec.Containers[0].Name
}

// sbctl runs `sbctl <args>` inside the webappapi pod and returns stdout.
func sbctl(f *framework.Framework, args string) string {
	ns, pod, container := webappAPIPod(f.ClientSet)
	return execInPod(f, ns, pod, container, "sbctl "+args)
}

// webappAPIPod locates the running control-plane API pod hosting the sbctl CLI.
func webappAPIPod(c kubernetes.Interface) (ns, name, container string) {
	pods, err := c.CoreV1().Pods(metav1.NamespaceAll).List(context.Background(), metav1.ListOptions{})
	framework.ExpectNoError(err, "list pods to find webappapi")
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.Status.Phase == corev1.PodRunning && strings.Contains(p.Name, "webappapi") {
			return p.Namespace, p.Name, p.Spec.Containers[0].Name
		}
	}
	framework.Failf("no running webappapi pod found")
	return "", "", ""
}

// nvmeConnectCommands extracts the `nvme connect ...` command lines emitted by
// `sbctl volume connect`.
func nvmeConnectCommands(out string) []string {
	var cmds []string
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "nvme connect") {
			cmds = append(cmds, line)
		}
	}
	return cmds
}

// liveCount returns the number of live paths for the volume's subsystem, or 0
// if the subsystem is gone.
func liveCount(f *framework.Framework, podName, container, lvolID string) int {
	s := subsystemForLvol(listSubsystems(f, podName, container), lvolID)
	if s == nil {
		return 0
	}
	return len(liveControllers(s))
}