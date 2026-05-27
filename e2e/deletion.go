package e2e

import (
	"context"
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/test/e2e/framework"
)

var _ = ginkgo.Describe("SPDKCSI-DELETION", func() {
	f := newTestFramework("spdkcsi")

	// -------------------------------------------------------------------------
	// Volume deletion
	// -------------------------------------------------------------------------

	ginkgo.It("PV is deleted after its PVC is removed (reclaimPolicy: Delete)", func() {
		ns := f.Namespace.Name

		ginkgo.By("create PVC and test pod")
		deployPVC(ns)
		deployTestPod(ns)
		// Register cleanup for the failure path; in the success path we
		// delete explicitly below so we can observe the PV lifecycle.
		ginkgo.DeferCleanup(func() { deletePVCAndTestPod(ns) })

		ginkgo.By("wait for test pod to be ready (PVC is bound)")
		framework.ExpectNoError(
			waitForTestPodReady(f.ClientSet, 5*time.Minute, ns, testPodName),
			"wait for test pod",
		)

		ginkgo.By("record the PV name from the bound PVC")
		pvc, err := f.ClientSet.CoreV1().PersistentVolumeClaims(ns).
			Get(context.Background(), "spdkcsi-pvc", metav1.GetOptions{})
		framework.ExpectNoError(err, "get PVC")
		pvName := pvc.Spec.VolumeName
		gomega.Expect(pvName).NotTo(gomega.BeEmpty(), "PVC must be bound to a PV")

		ginkgo.By("delete the test pod")
		deleteTestPod(ns)
		framework.ExpectNoError(
			waitForTestPodGone(f.ClientSet, ns, testPodName),
			"wait for test pod to terminate",
		)

		ginkgo.By("delete the PVC")
		deletePVC(ns)
		framework.ExpectNoError(
			waitForPvcGone(f.ClientSet, ns, "spdkcsi-pvc"),
			"wait for PVC to be deleted",
		)

		ginkgo.By("verify the backing PV is also deleted by the provisioner")
		framework.ExpectNoError(
			waitForPVDeleted(f.ClientSet, pvName, 3*time.Minute),
			"wait for PV to be reclaimed",
		)
	})

	// -------------------------------------------------------------------------
	// Snapshot deletion
	// -------------------------------------------------------------------------

	ginkgo.It("VolumeSnapshot is removed after explicit deletion", func() {
		ns := f.Namespace.Name
		const snapshotName = "spdk-snapshot-deletion-test"

		ginkgo.By("create source PVC and test pod")
		deployPVC(ns)
		deployTestPod(ns)
		ginkgo.DeferCleanup(func() { deletePVC(ns) }) // PVC outlives the test pod
		ginkgo.DeferCleanup(func() {
			// Best-effort cleanup in case test fails before explicit deletion.
			deleteSnapshotOnly(ns)
			deleteTestPod(ns)
		})

		ginkgo.By("wait for test pod to be ready and write data")
		testPodLabel := metav1.ListOptions{LabelSelector: "app=spdkcsi-pvc"}
		framework.ExpectNoError(
			waitForTestPodReady(f.ClientSet, 5*time.Minute, ns, testPodName),
			"wait for test pod",
		)
		writeDataToPod(f, ns, &testPodLabel, "snapshot deletion test data", "/spdkvol/test")

		ginkgo.By("delete source test pod (PVC stays alive for snapshotting)")
		deleteTestPod(ns)
		framework.ExpectNoError(
			waitForTestPodGone(f.ClientSet, ns, testPodName),
			"wait for test pod to terminate",
		)

		ginkgo.By("create VolumeSnapshot")
		deploySnapshotOnly(ns)

		ginkgo.By("wait for VolumeSnapshot to be ready")
		framework.ExpectNoError(
			waitForSnapshotReady(ns, snapshotName, 5*time.Minute),
			"wait for snapshot to be ready",
		)

		ginkgo.By("delete the VolumeSnapshot")
		deleteSnapshotOnly(ns)

		ginkgo.By("verify the VolumeSnapshot is fully removed")
		framework.ExpectNoError(
			waitForSnapshotGone(ns, snapshotName, 3*time.Minute),
			"wait for snapshot to be deleted",
		)
	})
})
