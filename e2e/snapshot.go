package e2e

import (
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/test/e2e/framework"
)

var _ = ginkgo.Describe("SPDKCSI-SNAPSHOT", func() {
	f := newTestFramework("spdkcsi")

	ginkgo.It("snapshot volumes preserve data from before each snapshot was taken", func() {
		ns := f.Namespace.Name
		testPodLabel := metav1.ListOptions{LabelSelector: "app=spdkcsi-pvc"}

		ginkgo.By("create source PVC")
		deployPVC(ns)
		ginkgo.DeferCleanup(func() { deletePVC(ns) })

		ginkgo.By("deploy test pod and write first data set")
		deployTestPod(ns)
		framework.ExpectNoError(
			waitForTestPodReady(f.ClientSet, 3*time.Minute, ns, testPodName),
			"wait for test pod",
		)
		writeDataToPod(f, ns, &testPodLabel, "Data that needs to be stored", "/spdkvol/test")
		deleteTestPod(ns)
		framework.ExpectNoError(
			waitForTestPodGone(f.ClientSet, ns, testPodName),
			"wait for test pod to terminate",
		)

		ginkgo.By("create snapshot1 and verify first data set")
		deploySnapshot(ns)
		ginkgo.DeferCleanup(func() {
			deleteSnapshot(ns)
			if err := waitForTestPodGone(f.ClientSet, ns, "spdkcsi-test-snapshot1"); err != nil {
				framework.Logf("timed out waiting for snapshot1 pod to terminate: %v", err)
			}
		})
		framework.ExpectNoError(
			waitForTestPodReady(f.ClientSet, 3*time.Minute, ns, "spdkcsi-test-snapshot1"),
			"wait for snapshot1 pod",
		)
		compareDataInPod(f, ns, &testPodLabel,
			[]string{"Data that needs to be stored"},
			[]string{"/spdkvol/test"},
		)

		ginkgo.By("write second data set to source PVC")
		deleteSnapshot(ns)
		framework.ExpectNoError(
			waitForTestPodGone(f.ClientSet, ns, "spdkcsi-test-snapshot1"),
			"wait for snapshot1 pod to terminate",
		)
		deployTestPod(ns)
		framework.ExpectNoError(
			waitForTestPodReady(f.ClientSet, 3*time.Minute, ns, testPodName),
			"wait for test pod",
		)
		writeDataToPod(f, ns, &testPodLabel, "Second data that needs to be stored", "/spdkvol/test2")
		deleteTestPod(ns)
		framework.ExpectNoError(
			waitForTestPodGone(f.ClientSet, ns, testPodName),
			"wait for test pod to terminate",
		)

		ginkgo.By("create snapshot2 and verify both data sets")
		deploySnapshot2(ns)
		ginkgo.DeferCleanup(func() { deleteSnapshot2(ns) })
		framework.ExpectNoError(
			waitForTestPodReady(f.ClientSet, 3*time.Minute, ns, "spdkcsi-test-snapshot2"),
			"wait for snapshot2 pod",
		)
		compareDataInPod(f, ns, &testPodLabel,
			[]string{"Data that needs to be stored", "Second data that needs to be stored"},
			[]string{"/spdkvol/test", "/spdkvol/test2"},
		)
	})
})
