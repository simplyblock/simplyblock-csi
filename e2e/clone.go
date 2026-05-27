package e2e

import (
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/test/e2e/framework"
)

var _ = ginkgo.Describe("SPDKCSI-CLONE", func() {
	f := newTestFramework("spdkcsi")

	ginkgo.It("cloned volume contains data written to the source volume", func() {
		ns := f.Namespace.Name
		testPodLabel := metav1.ListOptions{LabelSelector: "app=spdkcsi-pvc"}

		ginkgo.By("create source PVC")
		deployPVC(ns)
		ginkgo.DeferCleanup(func() { deletePVC(ns) })

		ginkgo.By("deploy test pod and write data to source PVC")
		deployTestPod(ns)
		framework.ExpectNoError(
			waitForTestPodReady(f.ClientSet, 3*time.Minute, ns, testPodName),
			"wait for source test pod",
		)
		writeDataToPod(f, ns, &testPodLabel, "Data that needs to be stored", "/spdkvol/test")

		// The clone API requires the source PVC to not be in use while cloning
		// on some backends.  Delete the test pod and wait for full termination
		// before creating the clone so there is no ambiguity about which pod
		// the label selector resolves to during verification.
		ginkgo.By("delete source test pod before cloning")
		deleteTestPod(ns)
		framework.ExpectNoError(
			waitForTestPodGone(f.ClientSet, ns, testPodName),
			"wait for source test pod to terminate",
		)

		ginkgo.By("create clone PVC and pod")
		deployClone(ns)
		ginkgo.DeferCleanup(func() { deleteClone(ns) })

		ginkgo.By("wait for clone pod to be ready")
		framework.ExpectNoError(
			waitForTestPodReady(f.ClientSet, 3*time.Minute, ns, "spdkcsi-test-clone"),
			"wait for clone pod",
		)

		ginkgo.By("verify clone contains the data written to the source")
		compareDataInPod(f, ns, &testPodLabel,
			[]string{"Data that needs to be stored"},
			[]string{"/spdkvol/test"},
		)
	})
})
