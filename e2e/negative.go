package e2e

import (
	"context"
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/test/e2e/framework"
)

var _ = ginkgo.Describe("SPDKCSI-NEGATIVE", func() {
	f := newTestFramework("spdkcsi")

	// -------------------------------------------------------------------------
	// Invalid StorageClass → PVC stays Pending
	// -------------------------------------------------------------------------

	ginkgo.It("PVC referencing a non-existent StorageClass stays Pending", func() {
		ns := f.Namespace.Name
		const pvcName = "spdkcsi-pvc-invalid-sc"

		ginkgo.By("create PVC referencing a StorageClass that does not exist")
		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      pvcName,
				Namespace: ns,
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("1Gi"),
					},
				},
				StorageClassName: func() *string {
					s := "spdkcsi-nonexistent-storageclass-xyz"
					return &s
				}(),
			},
		}
		_, err := f.ClientSet.CoreV1().PersistentVolumeClaims(ns).
			Create(context.Background(), pvc, metav1.CreateOptions{})
		framework.ExpectNoError(err, "create PVC with invalid StorageClass")
		ginkgo.DeferCleanup(func() {
			// Best-effort cleanup; ignore errors since PVC may already be gone.
			_ = f.ClientSet.CoreV1().PersistentVolumeClaims(ns).
				Delete(context.Background(), pvcName, metav1.DeleteOptions{})
		})

		ginkgo.By("verify the PVC remains in Pending phase for at least 30 seconds")
		assertPVCStaysPending(f.ClientSet, ns, pvcName, 30*time.Second)
	})

	// -------------------------------------------------------------------------
	// PVC shrink rejected by API server
	// -------------------------------------------------------------------------

	ginkgo.It("shrinking a PVC capacity is rejected by the API server", func() {
		ns := f.Namespace.Name

		ginkgo.By("create PVC and test pod")
		deployPVC(ns)
		deployTestPod(ns)
		ginkgo.DeferCleanup(func() { deletePVCAndTestPod(ns) })

		ginkgo.By("wait for test pod to be ready (PVC is bound)")
		framework.ExpectNoError(
			waitForTestPodReady(f.ClientSet, 5*time.Minute, ns, testPodName),
			"wait for test pod",
		)

		ginkgo.By("attempt to shrink the PVC — API server must reject it")
		pvc, err := f.ClientSet.CoreV1().PersistentVolumeClaims(ns).
			Get(context.Background(), "spdkcsi-pvc", metav1.GetOptions{})
		framework.ExpectNoError(err, "get PVC for shrink attempt")

		// Force a smaller size regardless of current allocation.
		smallSize := resource.MustParse("512Mi")
		pvc.Spec.Resources.Requests[corev1.ResourceStorage] = smallSize

		_, err = f.ClientSet.CoreV1().PersistentVolumeClaims(ns).
			Update(context.Background(), pvc, metav1.UpdateOptions{})
		gomega.Expect(err).To(gomega.HaveOccurred(),
			"API server must reject PVC capacity decrease")
	})

	// -------------------------------------------------------------------------
	// Clone from non-existent source → PVC stays Pending
	// -------------------------------------------------------------------------

	ginkgo.It("cloning from a non-existent source PVC leaves the clone PVC Pending", func() {
		ns := f.Namespace.Name
		const clonePVCName = "spdkcsi-clone-bad-source"

		ginkgo.By("create clone PVC referencing a source PVC that does not exist")
		dataSourceAPIGroup := ""
		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      clonePVCName,
				Namespace: ns,
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("1Gi"),
					},
				},
				StorageClassName: func() *string {
					s := storageClassName
					return &s
				}(),
				DataSource: &corev1.TypedLocalObjectReference{
					APIGroup: &dataSourceAPIGroup,
					Kind:     "PersistentVolumeClaim",
					Name:     "spdkcsi-nonexistent-source-pvc-xyz",
				},
			},
		}
		_, err := f.ClientSet.CoreV1().PersistentVolumeClaims(ns).
			Create(context.Background(), pvc, metav1.CreateOptions{})
		framework.ExpectNoError(err, "create clone PVC with non-existent source")
		ginkgo.DeferCleanup(func() {
			_ = f.ClientSet.CoreV1().PersistentVolumeClaims(ns).
				Delete(context.Background(), clonePVCName, metav1.DeleteOptions{})
		})

		ginkgo.By("verify the clone PVC remains in Pending phase for at least 30 seconds")
		assertPVCStaysPending(f.ClientSet, ns, clonePVCName, 30*time.Second)
	})
})
