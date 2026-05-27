package e2e

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	e2ekubectl "k8s.io/kubernetes/test/e2e/framework/kubectl"
)

var _ = ginkgo.Describe("SPDKCSI-MULTICLUSTER", func() {
	f := newTestFramework("spdkcsi-multicluster")

	ginkgo.BeforeEach(func() {
		if os.Getenv("MULTI_CLUSTER_E2E") != "true" {
			ginkgo.Skip("MULTI_CLUSTER_E2E=true is required for multi-cluster E2E tests")
		}
	})

	ginkgo.It("provisions volumes on the backend cluster selected by pod topology", func() {
		ns := f.Namespace.Name

		clusterRefs := envList("MULTI_CLUSTER_REFS", []string{"simplyblock-cluster-a", "simplyblock-cluster-b"})
		zones := envList("MULTI_CLUSTER_ZONES", []string{"multi-cluster-a", "multi-cluster-b"})
		poolName := envOrDefault("MULTI_CLUSTER_POOL_NAME", "pool1")
		storageClassNames := envList("MULTI_CLUSTER_STORAGE_CLASS_NAMES", nil)

		if len(clusterRefs) != 2 || len(zones) != 2 {
			ginkgo.Fail("MULTI_CLUSTER_REFS and MULTI_CLUSTER_ZONES must each contain exactly two comma-separated values")
		}
		if len(storageClassNames) == 0 {
			storageClassNames = deriveMultiClusterStorageClassNames(clusterRefs, poolName)
		}
		if len(storageClassNames) != 2 {
			ginkgo.Fail("MULTI_CLUSTER_STORAGE_CLASS_NAMES must contain exactly two comma-separated values when set")
		}

		clusterIDs := make([]string, len(clusterRefs))
		for i, clusterRef := range clusterRefs {
			clusterNamespace, clusterName := splitNamespacedRef(clusterRef, nameSpace)
			clusterID, err := waitForStorageClusterUUID(clusterNamespace, clusterName, 10*time.Minute)
			framework.ExpectNoError(err, "resolve cluster UUID for %s", clusterRef)
			clusterIDs[i] = clusterID
		}

		for i, zone := range zones {
			pvcName := fmt.Sprintf("spdkcsi-multicluster-pvc-%d", i+1)
			podName := fmt.Sprintf("spdkcsi-multicluster-pod-%d", i+1)
			scName := storageClassNames[i]

			ginkgo.By(fmt.Sprintf("provision PVC %s with StorageClass %s for zone %s", pvcName, scName, zone))
			framework.ExpectNoError(
				createPVC(f.ClientSet, ns, pvcName, scName, 1*1024*1024*1024),
				"create PVC %s", pvcName,
			)
			ginkgo.DeferCleanup(func() {
				gomega.Expect(deletePodIfExists(f.ClientSet, ns, podName)).To(gomega.Succeed())
				gomega.Expect(deletePVCIfExists(f.ClientSet, ns, pvcName)).To(gomega.Succeed())
			})

			framework.ExpectNoError(
				createTopologyPinnedPod(f.ClientSet, ns, podName, pvcName, zone),
				"create topology-pinned pod %s", podName,
			)

			framework.ExpectNoError(
				waitForTestPodReady(f.ClientSet, 5*time.Minute, ns, podName),
				"wait for pod %s", podName,
			)

			pv, err := waitForPVCBound(f.ClientSet, ns, pvcName, 5*time.Minute)
			framework.ExpectNoError(err, "wait for PVC %s to bind", pvcName)

			framework.ExpectNoError(
				verifyPVClusterAndTopology(pv, clusterIDs[i], zone),
				"verify PV cluster and topology for zone %s", zone,
			)

			opt := metav1.ListOptions{FieldSelector: "metadata.name=" + podName}
			writeDataToPod(f, ns, &opt, fmt.Sprintf("multi cluster data %d", i+1), "/spdkvol/test")
		}
	})
})

// ---------------------------------------------------------------------------
// Multi-cluster utility functions
// ---------------------------------------------------------------------------

func envOrDefault(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func envList(key string, fallback []string) []string {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	parts := strings.Split(raw, ",")
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		if value := strings.TrimSpace(part); value != "" {
			values = append(values, value)
		}
	}
	return values
}

func deriveMultiClusterStorageClassNames(clusterRefs []string, poolName string) []string {
	names := make([]string, 0, len(clusterRefs))
	for _, clusterRef := range clusterRefs {
		ns, clusterName := splitNamespacedRef(clusterRef, nameSpace)
		names = append(names, fmt.Sprintf("simplyblock-%s-%s-%s", ns, clusterName, poolName))
	}
	return names
}

func splitNamespacedRef(ref, fallbackNamespace string) (string, string) {
	parts := strings.SplitN(ref, "/", 2)
	if len(parts) == 2 {
		return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
	}
	return fallbackNamespace, strings.TrimSpace(ref)
}

func waitForStorageClusterUUID(namespace, clusterName string, timeout time.Duration) (string, error) {
	var uuid string
	err := wait.PollUntilContextTimeout(context.Background(), 10*time.Second, timeout, true,
		func(_ context.Context) (bool, error) {
			out, err := e2ekubectl.RunKubectl(namespace, "get",
				"storageclusters.storage.simplyblock.io", clusterName,
				"-o", "jsonpath={.status.uuid}")
			if err != nil {
				framework.Logf("waiting for StorageCluster %s/%s uuid: %v", namespace, clusterName, err)
				return false, nil
			}
			uuid = strings.TrimSpace(out)
			return uuid != "", nil
		})
	if err != nil {
		return "", fmt.Errorf("StorageCluster %s/%s uuid not available within %s: %w", namespace, clusterName, timeout, err)
	}
	return uuid, nil
}

func createTopologyPinnedPod(c kubernetes.Interface, namespace, podName, pvcClaimName, zone string) error {
	_, err := c.CoreV1().Pods(namespace).Create(context.Background(), &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: podName},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:    "spdk-csi-container",
					Image:   "busybox:latest",
					Command: []string{"sleep", "100000"},
					VolumeMounts: []corev1.VolumeMount{
						{Name: "spdk-csi-vol", MountPath: "/spdkvol"},
					},
				},
			},
			Affinity: &corev1.Affinity{
				NodeAffinity: &corev1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
						NodeSelectorTerms: []corev1.NodeSelectorTerm{
							{
								MatchExpressions: []corev1.NodeSelectorRequirement{
									{
										Key:      "topology.kubernetes.io/zone",
										Operator: corev1.NodeSelectorOpIn,
										Values:   []string{zone},
									},
								},
							},
						},
					},
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: "spdk-csi-vol",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: pvcClaimName,
						},
					},
				},
			},
		},
	}, metav1.CreateOptions{})
	return err
}

func waitForPVCBound(c kubernetes.Interface, ns, pvcName string, timeout time.Duration) (*corev1.PersistentVolume, error) {
	var pv *corev1.PersistentVolume
	err := wait.PollUntilContextTimeout(context.Background(), 3*time.Second, timeout, true,
		func(ctx context.Context) (bool, error) {
			pvc, err := c.CoreV1().PersistentVolumeClaims(ns).Get(ctx, pvcName, metav1.GetOptions{})
			if err != nil {
				return false, err
			}
			if pvc.Status.Phase != corev1.ClaimBound || pvc.Spec.VolumeName == "" {
				return false, nil
			}
			pv, err = c.CoreV1().PersistentVolumes().Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
			if err != nil {
				return false, err
			}
			return true, nil
		})
	if err != nil {
		return nil, fmt.Errorf("PVC %s did not bind within %s: %w", pvcName, timeout, err)
	}
	return pv, nil
}

func verifyPVClusterAndTopology(pv *corev1.PersistentVolume, expectedClusterID, expectedZone string) error {
	if pv.Spec.CSI == nil {
		return fmt.Errorf("PV %s does not have a CSI source", pv.Name)
	}
	if clusterID := pv.Spec.CSI.VolumeAttributes["cluster_id"]; clusterID != expectedClusterID {
		return fmt.Errorf("PV %s: cluster_id = %q, want %q", pv.Name, clusterID, expectedClusterID)
	}
	if zone := pv.Spec.CSI.VolumeAttributes["topology.kubernetes.io/zone"]; zone != "" && zone != expectedZone {
		return fmt.Errorf("PV %s: topology zone = %q, want %q", pv.Name, zone, expectedZone)
	}
	return nil
}

func deletePodIfExists(c kubernetes.Interface, ns, podName string) error {
	err := c.CoreV1().Pods(ns).Delete(context.Background(), podName, metav1.DeleteOptions{})
	if k8serrors.IsNotFound(err) {
		return nil
	}
	return err
}

func deletePVCIfExists(c kubernetes.Interface, ns, pvcName string) error {
	err := c.CoreV1().PersistentVolumeClaims(ns).Delete(context.Background(), pvcName, metav1.DeleteOptions{})
	if k8serrors.IsNotFound(err) {
		return nil
	}
	return err
}
