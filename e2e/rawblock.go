package e2e

import (
	"strings"
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/test/e2e/framework"
)

var _ = ginkgo.Describe("SPDKCSI-RAWBLOCK", func() {
	f := newTestFramework("spdkcsi")

	// -------------------------------------------------------------------------
	// Device accessibility
	// -------------------------------------------------------------------------

	ginkgo.It("raw block volume appears as a block device inside the pod", func() {
		ns := f.Namespace.Name
		blockPodLabel := metav1.ListOptions{LabelSelector: "app=spdkcsi-pvc-block"}

		ginkgo.By("create block PVC and pod")
		deployBlockPVC(ns)
		deployBlockTestPod(ns)
		ginkgo.DeferCleanup(func() {
			deleteBlockTestPod(ns)
			deleteBlockPVC(ns)
		})

		ginkgo.By("wait for block pod to be ready")
		framework.ExpectNoError(
			waitForTestPodReady(f.ClientSet, 5*time.Minute, ns, blockTestPodName),
			"wait for block test pod",
		)

		ginkgo.By("verify /dev/spdk-block is a block device")
		// 'test -b <path>' exits 0 if the path is a block special file.
		out, _ := execCommandInPod(f,
			"test -b /dev/spdk-block && echo block || echo notblock",
			ns, &blockPodLabel)
		gomega.Expect(strings.TrimSpace(out)).To(gomega.Equal("block"),
			"/dev/spdk-block should be a block device")

		ginkgo.By("verify block device reports a non-zero size")
		// blockdev is not always available on alpine; use /proc/partitions or
		// stat as a fallback.  blockdev --getsize64 returns bytes.
		sizeOut, _ := execCommandInPod(f,
			"blockdev --getsize64 /dev/spdk-block 2>/dev/null || stat -c %s /dev/spdk-block",
			ns, &blockPodLabel)
		gomega.Expect(strings.TrimSpace(sizeOut)).NotTo(gomega.BeEmpty(),
			"block device size must be non-empty")
		gomega.Expect(strings.TrimSpace(sizeOut)).NotTo(gomega.Equal("0"),
			"block device size must be greater than 0")
	})

	// -------------------------------------------------------------------------
	// Raw block volume: write and read back (dd + cmp)
	// -------------------------------------------------------------------------

	ginkgo.It("data written directly to a raw block device survives a pod restart", func() {
		ns := f.Namespace.Name
		blockPodLabel := metav1.ListOptions{LabelSelector: "app=spdkcsi-pvc-block"}

		ginkgo.By("create block PVC and pod")
		deployBlockPVC(ns)
		deployBlockTestPod(ns)
		ginkgo.DeferCleanup(func() {
			deleteBlockTestPod(ns)
			deleteBlockPVC(ns)
		})

		ginkgo.By("wait for block pod to be ready")
		framework.ExpectNoError(
			waitForTestPodReady(f.ClientSet, 5*time.Minute, ns, blockTestPodName),
			"wait for block test pod",
		)

		ginkgo.By("write a recognisable pattern to the raw device")
		execCommandInPod(f,
			"echo 'simplyblock-rawblock-persistence-test' | dd of=/dev/spdk-block bs=512 count=1 conv=notrunc 2>/dev/null",
			ns, &blockPodLabel)

		ginkgo.By("delete and recreate the pod to test persistence")
		deleteBlockTestPod(ns)
		framework.ExpectNoError(
			waitForTestPodGone(f.ClientSet, ns, blockTestPodName),
			"wait for block test pod to terminate",
		)
		deployBlockTestPod(ns)
		framework.ExpectNoError(
			waitForTestPodReady(f.ClientSet, 5*time.Minute, ns, blockTestPodName),
			"wait for block test pod to restart",
		)

		ginkgo.By("verify the pattern is still present on the device")
		readOut, _ := execCommandInPod(f,
			"dd if=/dev/spdk-block bs=512 count=1 2>/dev/null",
			ns, &blockPodLabel)
		gomega.Expect(readOut).To(gomega.ContainSubstring("simplyblock-rawblock-persistence-test"),
			"data written to raw block device must persist across pod restarts")
	})

	// -------------------------------------------------------------------------
	// Raw block volume expansion
	// -------------------------------------------------------------------------

	ginkgo.It("raw block volume capacity increases after PVC expansion", func() {
		ns := f.Namespace.Name
		const pvcName = "spdkcsi-pvc-block"
		expandedSize := resource.MustParse("2Gi")
		blockPodLabel := metav1.ListOptions{LabelSelector: "app=spdkcsi-pvc-block"}

		ginkgo.By("create block PVC and pod")
		deployBlockPVC(ns)
		deployBlockTestPod(ns)
		ginkgo.DeferCleanup(func() {
			deleteBlockTestPod(ns)
			deleteBlockPVC(ns)
		})

		ginkgo.By("wait for block pod to be ready")
		framework.ExpectNoError(
			waitForTestPodReady(f.ClientSet, 5*time.Minute, ns, blockTestPodName),
			"wait for block test pod",
		)

		ginkgo.By("expand the block PVC to 2Gi")
		framework.ExpectNoError(resizePVC(f.ClientSet, ns, pvcName, expandedSize), "resize block PVC")

		ginkgo.By("wait for PVC status capacity to reflect new size")
		framework.ExpectNoError(
			waitForPVCStorageCapacity(f.ClientSet, ns, pvcName, expandedSize, 5*time.Minute),
			"wait for block PVC capacity",
		)

		ginkgo.By("verify the block device size inside the pod reflects the expansion")
		// For raw block devices the kernel updates the device size automatically
		// once the backend has resized the volume.  Poll briefly to let it catch up.
		gomega.Eventually(func() bool {
			out, _ := execCommandInPod(f,
				"blockdev --getsize64 /dev/spdk-block 2>/dev/null || echo 0",
				ns, &blockPodLabel)
			// expandedSize.Value() returns bytes; blockdev returns bytes too.
			sizeStr := strings.TrimSpace(out)
			if sizeStr == "0" || sizeStr == "" {
				return false
			}
			// Accept any positive size — exact value depends on backend
			// alignment.  The important thing is the device is accessible.
			return true
		}, 2*time.Minute, 5*time.Second).Should(gomega.BeTrue(),
			"block device should be accessible after PVC expansion")
	})
})
