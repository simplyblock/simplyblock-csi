package e2e

import (
	"context"
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/test/e2e/framework"
)

var _ = ginkgo.Describe("SPDKCSI-PARAMS", func() {
	f := newTestFramework("spdkcsi")

	// -------------------------------------------------------------------------
	// QoS parameter: qos_rw_iops
	// -------------------------------------------------------------------------

	ginkgo.It("StorageClass with qos_rw_iops=1000 provisions a usable volume", func() {
		ns := f.Namespace.Name
		const scName = "spdkcsi-e2e-qos-iops"

		ginkgo.By("create StorageClass with qos_rw_iops parameter")
		createStorageClassWithParams(f.ClientSet, scName, map[string]string{
			"qos_rw_iops": "1000",
		})
		ginkgo.DeferCleanup(func() { deleteStorageClass(f.ClientSet, scName) })

		ginkgo.By("create PVC using QoS StorageClass")
		framework.ExpectNoError(
			createPVC(f.ClientSet, ns, "spdkcsi-pvc-qos-iops", scName, 1<<30),
			"create QoS IOPS PVC",
		)
		ginkgo.DeferCleanup(func() {
			framework.ExpectNoError(
				f.ClientSet.CoreV1().PersistentVolumeClaims(ns).
					Delete(context.Background(), "spdkcsi-pvc-qos-iops", metav1.DeleteOptions{}),
			)
		})

		ginkgo.By("create pod that mounts the QoS volume")
		framework.ExpectNoError(
			createPodForPVC(f.ClientSet, ns, "spdkcsi-test-qos-iops", "spdkcsi-pvc-qos-iops"),
			"create QoS IOPS test pod",
		)
		ginkgo.DeferCleanup(func() { deletePodByName(f.ClientSet, ns, "spdkcsi-test-qos-iops") })

		ginkgo.By("wait for pod to be ready (confirms volume was provisioned and attached)")
		framework.ExpectNoError(
			waitForTestPodReady(f.ClientSet, 5*time.Minute, ns, "spdkcsi-test-qos-iops"),
			"wait for QoS IOPS test pod",
		)

		ginkgo.By("verify the volume is writable")
		qosPodLabel := metav1.ListOptions{LabelSelector: "app=spdkcsi-test-qos-iops"}
		_, stderr := execCommandInPod(f, "echo qos-iops-ok > /spdkvol/probe", ns, &qosPodLabel)
		gomega.Expect(stderr).To(gomega.BeEmpty(), "write to QoS IOPS volume should succeed")
	})

	// -------------------------------------------------------------------------
	// QoS parameter: qos_rw_mbytes
	// -------------------------------------------------------------------------

	ginkgo.It("StorageClass with qos_rw_mbytes=100 provisions a usable volume", func() {
		ns := f.Namespace.Name
		const scName = "spdkcsi-e2e-qos-mbytes"

		ginkgo.By("create StorageClass with qos_rw_mbytes parameter")
		createStorageClassWithParams(f.ClientSet, scName, map[string]string{
			"qos_rw_mbytes": "100",
		})
		ginkgo.DeferCleanup(func() { deleteStorageClass(f.ClientSet, scName) })

		ginkgo.By("create PVC using QoS MB/s StorageClass")
		framework.ExpectNoError(
			createPVC(f.ClientSet, ns, "spdkcsi-pvc-qos-mbytes", scName, 1<<30),
			"create QoS MBytes PVC",
		)
		ginkgo.DeferCleanup(func() {
			framework.ExpectNoError(
				f.ClientSet.CoreV1().PersistentVolumeClaims(ns).
					Delete(context.Background(), "spdkcsi-pvc-qos-mbytes", metav1.DeleteOptions{}),
			)
		})

		ginkgo.By("create pod that mounts the QoS MB/s volume")
		framework.ExpectNoError(
			createPodForPVC(f.ClientSet, ns, "spdkcsi-test-qos-mbytes", "spdkcsi-pvc-qos-mbytes"),
			"create QoS MBytes test pod",
		)
		ginkgo.DeferCleanup(func() { deletePodByName(f.ClientSet, ns, "spdkcsi-test-qos-mbytes") })

		ginkgo.By("wait for pod to be ready")
		framework.ExpectNoError(
			waitForTestPodReady(f.ClientSet, 5*time.Minute, ns, "spdkcsi-test-qos-mbytes"),
			"wait for QoS MBytes test pod",
		)

		ginkgo.By("verify the volume is writable")
		qosPodLabel := metav1.ListOptions{LabelSelector: "app=spdkcsi-test-qos-mbytes"}
		_, stderr := execCommandInPod(f, "echo qos-mbytes-ok > /spdkvol/probe", ns, &qosPodLabel)
		gomega.Expect(stderr).To(gomega.BeEmpty(), "write to QoS MBytes volume should succeed")
	})

	// -------------------------------------------------------------------------
	// QoS parameter: PVC annotation override
	// -------------------------------------------------------------------------

	ginkgo.It("PVC annotation simplyblock.io/qos-rw-iops overrides StorageClass QoS", func() {
		ns := f.Namespace.Name
		const (
			scName  = "spdkcsi-e2e-qos-ann"
			pvcName = "spdkcsi-pvc-qos-ann"
			podName = "spdkcsi-test-qos-ann"
		)

		ginkgo.By("create base StorageClass (no QoS)")
		createStorageClassWithParams(f.ClientSet, scName, map[string]string{})
		ginkgo.DeferCleanup(func() { deleteStorageClass(f.ClientSet, scName) })

		ginkgo.By("create PVC with QoS annotation override")
		framework.ExpectNoError(
			createAnnotatedPVC(f.ClientSet, ns, pvcName, scName,
				resource.MustParse("1Gi"),
				map[string]string{"simplyblock.io/qos-rw-iops": "500"},
			),
			"create annotated QoS PVC",
		)
		ginkgo.DeferCleanup(func() {
			framework.ExpectNoError(
				f.ClientSet.CoreV1().PersistentVolumeClaims(ns).
					Delete(context.Background(), pvcName, metav1.DeleteOptions{}),
			)
		})

		ginkgo.By("create pod that mounts the annotated PVC")
		framework.ExpectNoError(
			createPodForPVC(f.ClientSet, ns, podName, pvcName),
			"create annotated QoS test pod",
		)
		ginkgo.DeferCleanup(func() { deletePodByName(f.ClientSet, ns, podName) })

		ginkgo.By("wait for pod to be ready (annotation accepted by provisioner)")
		framework.ExpectNoError(
			waitForTestPodReady(f.ClientSet, 5*time.Minute, ns, podName),
			"wait for annotated QoS test pod",
		)

		ginkgo.By("verify the volume is writable")
		podLabel := metav1.ListOptions{LabelSelector: "app=" + podName}
		_, stderr := execCommandInPod(f, "echo qos-ann-ok > /spdkvol/probe", ns, &podLabel)
		gomega.Expect(stderr).To(gomega.BeEmpty(), "write to annotated QoS volume should succeed")
	})

	// -------------------------------------------------------------------------
	// Encryption parameter
	// -------------------------------------------------------------------------

	ginkgo.It("StorageClass with encryption=True provisions and mounts an encrypted volume", func() {
		ns := f.Namespace.Name
		const scName = "spdkcsi-e2e-encrypted"

		ginkgo.By("create StorageClass with encryption parameter")
		createStorageClassWithParams(f.ClientSet, scName, map[string]string{
			"encryption": "True",
		})
		ginkgo.DeferCleanup(func() { deleteStorageClass(f.ClientSet, scName) })

		ginkgo.By("create PVC using encrypted StorageClass")
		framework.ExpectNoError(
			createPVC(f.ClientSet, ns, "spdkcsi-pvc-enc", scName, 1<<30),
			"create encrypted PVC",
		)
		ginkgo.DeferCleanup(func() {
			framework.ExpectNoError(
				f.ClientSet.CoreV1().PersistentVolumeClaims(ns).
					Delete(context.Background(), "spdkcsi-pvc-enc", metav1.DeleteOptions{}),
			)
		})

		ginkgo.By("create pod that mounts the encrypted volume")
		framework.ExpectNoError(
			createPodForPVC(f.ClientSet, ns, "spdkcsi-test-enc", "spdkcsi-pvc-enc"),
			"create encrypted test pod",
		)
		ginkgo.DeferCleanup(func() { deletePodByName(f.ClientSet, ns, "spdkcsi-test-enc") })

		ginkgo.By("wait for pod to be ready (encrypted volume mounted successfully)")
		framework.ExpectNoError(
			waitForTestPodReady(f.ClientSet, 5*time.Minute, ns, "spdkcsi-test-enc"),
			"wait for encrypted test pod",
		)

		ginkgo.By("verify the encrypted volume is readable and writable")
		encPodLabel := metav1.ListOptions{LabelSelector: "app=spdkcsi-test-enc"}
		_, stderr := execCommandInPod(f, "echo encrypted-ok > /spdkvol/probe && cat /spdkvol/probe",
			ns, &encPodLabel)
		gomega.Expect(stderr).To(gomega.BeEmpty(), "write/read on encrypted volume should succeed")
	})
})
