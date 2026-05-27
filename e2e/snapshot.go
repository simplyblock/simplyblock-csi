package e2e

import (
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/test/e2e/framework"
)

// The snapshot suite is Ordered: each It is a sequential step that builds on
// the previous one.  BeforeAll/AfterAll manage the shared source PVC lifetime.
// DeferCleanup in each It scopes that step's resources to the step itself.
var _ = ginkgo.Describe("SPDKCSI-SNAPSHOT", ginkgo.Ordered, func() {
	f := framework.NewDefaultFramework("spdkcsi")

	// Label shared by all test pods deployed in this suite.
	testPodLabel := metav1.ListOptions{LabelSelector: "app=spdkcsi-pvc"}

	// The source PVC is created before the first step and lives until AfterAll.
	ginkgo.BeforeAll(func() {
		ginkgo.By("create source PVC")
		deployPVC()
	})

	ginkgo.AfterAll(func() {
		ginkgo.By("delete source PVC")
		deletePVC()
	})

	ginkgo.It("writes initial data to the source PVC", func() {
		ginkgo.By("deploy test pod")
		deployTestPod()
		ginkgo.DeferCleanup(func() {
			deleteTestPod()
			// Wait for full termination so the snapshot1 step sees only its own pod.
			if err := waitForTestPodGone(f.ClientSet, testPodName); err != nil {
				framework.Logf("timed out waiting for test pod to terminate: %v", err)
			}
		})

		ginkgo.By("wait for test pod to be ready")
		framework.ExpectNoError(
			waitForTestPodReady(f.ClientSet, 3*time.Minute, testPodName),
			"wait for test pod",
		)

		ginkgo.By("write first data set to source PVC")
		writeDataToPod(f, &testPodLabel, "Data that needs to be stored", "/spdkvol/test")
	})

	ginkgo.It("snapshot1 contains data written before it was taken", func() {
		ginkgo.By("create VolumeSnapshot and restore PVC")
		deploySnapshot()
		ginkgo.DeferCleanup(func() {
			deleteSnapshot()
			if err := waitForTestPodGone(f.ClientSet, "spdkcsi-test-snapshot1"); err != nil {
				framework.Logf("timed out waiting for snapshot1 pod to terminate: %v", err)
			}
		})

		ginkgo.By("wait for snapshot1 restore pod to be ready")
		framework.ExpectNoError(
			waitForTestPodReady(f.ClientSet, 3*time.Minute, "spdkcsi-test-snapshot1"),
			"wait for snapshot1 pod",
		)

		ginkgo.By("verify snapshot1 contains the first data set")
		compareDataInPod(f, &testPodLabel,
			[]string{"Data that needs to be stored"},
			[]string{"/spdkvol/test"},
		)
	})

	ginkgo.It("writes second data to the source PVC after snapshot1", func() {
		ginkgo.By("deploy test pod")
		deployTestPod()
		ginkgo.DeferCleanup(func() {
			deleteTestPod()
			// Wait for full termination so the snapshot2 step sees only its own pod.
			if err := waitForTestPodGone(f.ClientSet, testPodName); err != nil {
				framework.Logf("timed out waiting for test pod to terminate: %v", err)
			}
		})

		ginkgo.By("wait for test pod to be ready")
		framework.ExpectNoError(
			waitForTestPodReady(f.ClientSet, 3*time.Minute, testPodName),
			"wait for test pod",
		)

		ginkgo.By("write second data set to source PVC")
		writeDataToPod(f, &testPodLabel, "Second data that needs to be stored", "/spdkvol/test2")
	})

	ginkgo.It("snapshot2 contains both data sets written before it was taken", func() {
		ginkgo.By("create second VolumeSnapshot and restore PVC")
		deploySnapshot2()
		ginkgo.DeferCleanup(deleteSnapshot2)

		ginkgo.By("wait for snapshot2 restore pod to be ready")
		framework.ExpectNoError(
			waitForTestPodReady(f.ClientSet, 3*time.Minute, "spdkcsi-test-snapshot2"),
			"wait for snapshot2 pod",
		)

		ginkgo.By("verify snapshot2 contains both data sets")
		compareDataInPod(f, &testPodLabel,
			[]string{"Data that needs to be stored", "Second data that needs to be stored"},
			[]string{"/spdkvol/test", "/spdkvol/test2"},
		)
	})
})
