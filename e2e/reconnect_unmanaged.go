package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
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

var _ = ginkgo.Describe("SPDKCSI-RECONNECT-UNMANAGED", func() {
	f := newTestFramework("spdkcsi")

	ginkgo.Context("NVMe-oF path recovery is gated on PV/PVC management", func() {
		// A simplyblock volume created directly via the API (no PV/PVC) is still
		// an NVMe-oF subsystem on the host, but the node plugin must NOT manage
		// its paths. We connect such a volume, degrade it, and confirm the
		// monitor leaves it alone (no recovery) — the behavior the positive
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
			framework.Logf("sbctl volume add %s: %s", volName, addOut)
			// `sbctl volume add` echoes a transient task id, not the volume's Id,
			// so resolve the real Id by name from the volume list.
			volID := sbctlVolumeIDByName(f, volName)
			gomega.Expect(volID).NotTo(gomega.BeEmpty(), "volume %q not found in sbctl volume list", volName)
			framework.Logf("created unmanaged volume %s (id %s)", volName, volID)
			// Delete the backend volume even if later steps fail.
			ginkgo.DeferCleanup(func() {
				out := sbctl(f, "volume delete "+volID+" --force")
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
// its plugin container.
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
	return pod.Spec.NodeName, pod.Name, pluginContainerName(&pod)
}

// pluginContainerName returns the node plugin container (the one carrying the
// spdkcsi binary, nvme-cli and a shell) rather than a sidecar such as
// csi-registrar or liveness-probe, which run on distroless images with no shell.
func pluginContainerName(pod *corev1.Pod) string {
	for _, c := range pod.Spec.Containers {
		switch c.Name {
		case "csi-registrar", "node-driver-registrar", "liveness-probe":
			continue
		default:
			return c.Name
		}
	}
	return pod.Spec.Containers[0].Name
}

// sbctlVolumeIDByName resolves a simplyblock volume's Id from its name via
// `sbctl volume list --json`. Returns "" if no volume with that name exists.
func sbctlVolumeIDByName(f *framework.Framework, name string) string {
	out := sbctl(f, "volume list --json")
	// sbctl may prefix log lines before the JSON array; slice from the first '['.
	if i := strings.IndexByte(out, '['); i > 0 {
		out = out[i:]
	}
	var vols []struct {
		ID   string `json:"Id"`
		Name string `json:"Name"`
	}
	if err := json.Unmarshal([]byte(out), &vols); err != nil {
		framework.Failf("parse sbctl volume list --json %q: %v", out, err)
	}
	for _, v := range vols {
		if v.Name == name {
			return v.ID
		}
	}
	return ""
}

// sbctlClusterID resolves a simplyblock cluster's UUID via `sbctl cluster list
// --json`. It matches by name when name != "", otherwise (or if the name has no
// match) falls back to the sole cluster. Returns "" if it cannot resolve one.
func sbctlClusterID(f *framework.Framework, name string) string {
	out := sbctl(f, "cluster list --json")
	if i := strings.IndexByte(out, '['); i > 0 {
		out = out[i:]
	}
	var clusters []struct {
		UUID string `json:"UUID"`
		Name string `json:"Name"`
	}
	if err := json.Unmarshal([]byte(out), &clusters); err != nil {
		framework.Failf("parse sbctl cluster list --json %q: %v", out, err)
	}
	for _, c := range clusters {
		if c.Name == name {
			return c.UUID
		}
	}
	if len(clusters) == 1 {
		return clusters[0].UUID
	}
	return ""
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
// `sbctl volume connect`. The CLI prints them as `sudo nvme connect ... \`
// (sudo prefix, trailing line-continuation backslash), both of which are
// stripped so the command can run directly in the csi-node container.
func nvmeConnectCommands(out string) []string {
	var cmds []string
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		line = strings.TrimSpace(strings.TrimSuffix(line, "\\"))
		line = strings.TrimPrefix(line, "sudo ")
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
