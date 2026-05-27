package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
	"k8s.io/kubernetes/test/e2e/framework"
	e2ekubectl "k8s.io/kubernetes/test/e2e/framework/kubectl"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
)

var (
	nameSpace       string
	storageClassName string
	operatorMode    bool
	systemNamespace string
)

const (
	// Template YAML paths (relative to the e2e/ directory).
	pvcPath                  = "templates/pvc.yaml"
	cachepvcPath             = "templates/pvc-cache.yaml"
	testPodPath              = "templates/testpod.yaml"
	cachetestPodPath         = "templates/testpod-cache.yaml"
	multiPvcsPath            = "templates/multi-pvc.yaml"
	testPodWithMultiPvcsPath = "templates/testpod-multi-pvc.yaml"
	testPodWithSnapshotPath  = "templates/testpod-snapshot.yaml"
	testPodWithSnapshotPath2 = "templates/testpod-snapshot2.yaml"
	testPodWithClonePath     = "templates/testpod-clone.yaml"

	// Kubernetes resource names.
	controllerStsName = "simplyblock-csi-controller"
	nodeDsName        = "simplyblock-csi-node"
	testPodName       = "spdkcsi-test"
	multiTestPodName  = "spdkcsi-test-multi"
	cachetestPodName  = "spdkcsi-cache-test"
)

func init() {
	nameSpace = os.Getenv("CSI_NAMESPACE")
	if nameSpace == "" {
		nameSpace = "default"
	}
	storageClassName = os.Getenv("STORAGE_CLASS_NAME")
	if storageClassName == "" {
		storageClassName = "simplyblock-csi-sc"
	}
	operatorMode = os.Getenv("OPERATOR_MODE") == "true"
	systemNamespace = os.Getenv("CSI_SYSTEM_NAMESPACE")
	if systemNamespace == "" {
		systemNamespace = "simplyblock"
	}
}

// applyTemplateWithStorageClass applies a YAML template after substituting
// the default storage class name with the one configured via STORAGE_CLASS_NAME.
func applyTemplateWithStorageClass(ns, path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	modified := strings.ReplaceAll(string(data), "simplyblock-csi-sc", storageClassName)
	tmp, err := os.CreateTemp("", "e2e-*.yaml")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err = tmp.WriteString(modified); err != nil {
		return err
	}
	tmp.Close()
	_, err = e2ekubectl.RunKubectl(ns, "apply", "-f", tmp.Name())
	return err
}

// ---------------------------------------------------------------------------
// Deploy helpers — fail the test immediately on error.
// ---------------------------------------------------------------------------

func deployTestPod() {
	_, err := e2ekubectl.RunKubectl(nameSpace, "apply", "-f", testPodPath)
	framework.ExpectNoError(err, "deploy test pod")
}

func deployPVC() {
	framework.ExpectNoError(applyTemplateWithStorageClass(nameSpace, pvcPath), "deploy PVC")
}

func deploySnapshot() {
	framework.ExpectNoError(applyTemplateWithStorageClass(nameSpace, testPodWithSnapshotPath), "deploy snapshot resources")
}

func deploySnapshot2() {
	framework.ExpectNoError(applyTemplateWithStorageClass(nameSpace, testPodWithSnapshotPath2), "deploy snapshot2 resources")
}

func deployClone() {
	framework.ExpectNoError(applyTemplateWithStorageClass(nameSpace, testPodWithClonePath), "deploy clone resources")
}

func deployTestPodWithMultiPvcs() {
	_, err := e2ekubectl.RunKubectl(nameSpace, "apply", "-f", testPodWithMultiPvcsPath)
	framework.ExpectNoError(err, "deploy test pod with multi-PVCs")
}

func deployMultiPvcs() {
	framework.ExpectNoError(applyTemplateWithStorageClass(nameSpace, multiPvcsPath), "deploy multi-PVCs")
}

// ---------------------------------------------------------------------------
// Delete helpers — best-effort; log but do not fail on error so that a
// cleanup hiccup does not shadow a legitimate test failure.
// ---------------------------------------------------------------------------

func deleteTestPod() {
	if _, err := e2ekubectl.RunKubectl(nameSpace, "delete", "-f", testPodPath); err != nil {
		framework.Logf("failed to delete test pod: %v", err)
	}
}

func deletePVC() {
	if _, err := e2ekubectl.RunKubectl(nameSpace, "delete", "-f", pvcPath); err != nil {
		framework.Logf("failed to delete PVC: %v", err)
	}
}

func deleteSnapshot() {
	if _, err := e2ekubectl.RunKubectl(nameSpace, "delete", "-f", testPodWithSnapshotPath); err != nil {
		framework.Logf("failed to delete snapshot resources: %v", err)
	}
}

func deleteSnapshot2() {
	if _, err := e2ekubectl.RunKubectl(nameSpace, "delete", "-f", testPodWithSnapshotPath2); err != nil {
		framework.Logf("failed to delete snapshot2 resources: %v", err)
	}
}

func deleteClone() {
	if _, err := e2ekubectl.RunKubectl(nameSpace, "delete", "-f", testPodWithClonePath); err != nil {
		framework.Logf("failed to delete clone resources: %v", err)
	}
}

func deletePVCAndTestPod() {
	deleteTestPod()
	deletePVC()
}

func deleteTestPodWithMultiPvcs() {
	if _, err := e2ekubectl.RunKubectl(nameSpace, "delete", "-f", testPodWithMultiPvcsPath); err != nil {
		framework.Logf("failed to delete multi-PVC test pod: %v", err)
	}
}

func deleteMultiPvcs() {
	if _, err := e2ekubectl.RunKubectl(nameSpace, "delete", "-f", multiPvcsPath); err != nil {
		framework.Logf("failed to delete multi-PVCs: %v", err)
	}
}

func deleteMultiPvcsAndTestPodWithMultiPvcs() {
	deleteTestPodWithMultiPvcs()
	deleteMultiPvcs()
}

// ---------------------------------------------------------------------------
// Wait helpers — all use wait.PollUntilContextTimeout (replaces deprecated
// wait.PollImmediate).
// ---------------------------------------------------------------------------

func waitForControllerReady(c kubernetes.Interface, timeout time.Duration) error {
	ns := nameSpace
	if operatorMode {
		ns = systemNamespace
	}
	err := wait.PollUntilContextTimeout(context.Background(), 3*time.Second, timeout, true,
		func(ctx context.Context) (bool, error) {
			sts, err := c.AppsV1().StatefulSets(ns).Get(ctx, controllerStsName, metav1.GetOptions{})
			if err != nil {
				return false, err
			}
			return sts.Status.Replicas == sts.Status.ReadyReplicas, nil
		})
	if err != nil {
		return fmt.Errorf("controller StatefulSet %q not ready within %s: %w", controllerStsName, timeout, err)
	}
	return nil
}

func waitForNodeServerReady(c kubernetes.Interface, timeout time.Duration) error {
	ns := nameSpace
	if operatorMode {
		ns = systemNamespace
	}
	err := wait.PollUntilContextTimeout(context.Background(), 3*time.Second, timeout, true,
		func(ctx context.Context) (bool, error) {
			ds, err := c.AppsV1().DaemonSets(ns).Get(ctx, nodeDsName, metav1.GetOptions{})
			if err != nil {
				return false, err
			}
			return ds.Status.NumberReady == ds.Status.DesiredNumberScheduled, nil
		})
	if err != nil {
		return fmt.Errorf("node DaemonSet %q not ready within %s: %w", nodeDsName, timeout, err)
	}
	return nil
}

// waitForTestPodReady polls until podName is Running with every container
// reporting Ready, or until timeout.  It returns immediately with an error if
// the pod enters a terminal phase (Failed/Succeeded).
func waitForTestPodReady(c kubernetes.Interface, timeout time.Duration, podName string) error {
	err := wait.PollUntilContextTimeout(context.Background(), 3*time.Second, timeout, true,
		func(ctx context.Context) (bool, error) {
			pod, err := c.CoreV1().Pods(nameSpace).Get(ctx, podName, metav1.GetOptions{})
			if err != nil {
				return false, err
			}
			switch pod.Status.Phase {
			case corev1.PodFailed, corev1.PodSucceeded:
				return false, fmt.Errorf("pod %q entered terminal phase %s", podName, pod.Status.Phase)
			case corev1.PodRunning:
				if len(pod.Status.ContainerStatuses) == 0 {
					return false, nil
				}
				for _, cs := range pod.Status.ContainerStatuses {
					if !cs.Ready {
						return false, nil
					}
				}
				return true, nil
			default:
				return false, nil
			}
		})
	if err != nil {
		return fmt.Errorf("pod %q not ready within %s: %w", podName, timeout, err)
	}
	return nil
}

func waitForTestPodGone(c kubernetes.Interface, podName string) error {
	err := wait.PollUntilContextTimeout(context.Background(), 3*time.Second, 5*time.Minute, true,
		func(ctx context.Context) (bool, error) {
			_, err := c.CoreV1().Pods(nameSpace).Get(ctx, podName, metav1.GetOptions{})
			if k8serrors.IsNotFound(err) {
				return true, nil
			}
			return false, err
		})
	if err != nil {
		return fmt.Errorf("pod %q still present after 5 minutes: %w", podName, err)
	}
	return nil
}

func waitForPvcGone(c kubernetes.Interface, pvcName string) error {
	err := wait.PollUntilContextTimeout(context.Background(), 3*time.Second, 5*time.Minute, true,
		func(ctx context.Context) (bool, error) {
			_, err := c.CoreV1().PersistentVolumeClaims(nameSpace).Get(ctx, pvcName, metav1.GetOptions{})
			if k8serrors.IsNotFound(err) {
				return true, nil
			}
			return false, err
		})
	if err != nil {
		return fmt.Errorf("PVC %q still present after 5 minutes: %w", pvcName, err)
	}
	return nil
}

func waitForPVCStorageCapacity(c kubernetes.Interface, pvcName string, minSize resource.Quantity, timeout time.Duration) error {
	err := wait.PollUntilContextTimeout(context.Background(), 3*time.Second, timeout, true,
		func(ctx context.Context) (bool, error) {
			pvc, err := c.CoreV1().PersistentVolumeClaims(nameSpace).Get(ctx, pvcName, metav1.GetOptions{})
			if err != nil {
				return false, err
			}
			capacity, ok := pvc.Status.Capacity[corev1.ResourceStorage]
			if !ok {
				return false, nil
			}
			return capacity.Cmp(minSize) >= 0, nil
		})
	if err != nil {
		return fmt.Errorf("PVC %q capacity did not reach %s within %s: %w", pvcName, minSize.String(), timeout, err)
	}
	return nil
}

func waitForFilesystemSize(f *framework.Framework, opt *metav1.ListOptions, mountPath string, minBytes int64, timeout time.Duration) error {
	err := wait.PollUntilContextTimeout(context.Background(), 5*time.Second, timeout, true,
		func(_ context.Context) (bool, error) {
			sizeBytes, err := filesystemSizeBytes(f, opt, mountPath)
			if err != nil {
				framework.Logf("filesystem size check failed: %v", err)
				return false, err
			}
			return sizeBytes >= minBytes, nil
		})
	if err != nil {
		return fmt.Errorf("filesystem at %q did not reach %d bytes within %s: %w", mountPath, minBytes, timeout, err)
	}
	return nil
}

func waitForMountedVolumeStats(c kubernetes.Interface, podName string, timeout time.Duration) error {
	err := wait.PollUntilContextTimeout(context.Background(), 10*time.Second, timeout, true,
		func(ctx context.Context) (bool, error) {
			pod, err := c.CoreV1().Pods(nameSpace).Get(ctx, podName, metav1.GetOptions{})
			if err != nil {
				return false, err
			}
			if pod.Spec.NodeName == "" {
				return false, nil
			}

			raw, err := c.CoreV1().RESTClient().Get().
				Resource("nodes").
				Name(pod.Spec.NodeName).
				SubResource("proxy").
				Suffix("stats", "summary").
				DoRaw(ctx)
			if err != nil {
				// Kubelet stats may not be immediately available; keep polling.
				framework.Logf("kubelet stats not yet available on node %s: %v", pod.Spec.NodeName, err)
				return false, nil
			}

			var summary kubeletStatsSummary
			if err := json.Unmarshal(raw, &summary); err != nil {
				return false, fmt.Errorf("parse kubelet stats summary: %w", err)
			}

			for _, podStats := range summary.Pods {
				if podStats.PodRef.Namespace != nameSpace || podStats.PodRef.Name != podName {
					continue
				}
				for _, vol := range podStats.VolumeStats {
					if vol.CapacityBytes != nil && *vol.CapacityBytes > 0 &&
						vol.AvailableBytes != nil && vol.UsedBytes != nil {
						return true, nil
					}
				}
				return false, nil
			}
			return false, nil
		})
	if err != nil {
		return fmt.Errorf("kubelet volume stats for pod %q not populated within %s: %w", podName, timeout, err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// PVC helpers
// ---------------------------------------------------------------------------

func resizePVC(c kubernetes.Interface, pvcName string, newSize resource.Quantity) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		pvc, err := c.CoreV1().PersistentVolumeClaims(nameSpace).Get(context.Background(), pvcName, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if pvc.Spec.Resources.Requests == nil {
			pvc.Spec.Resources.Requests = corev1.ResourceList{}
		}
		pvc.Spec.Resources.Requests[corev1.ResourceStorage] = newSize
		_, err = c.CoreV1().PersistentVolumeClaims(nameSpace).Update(context.Background(), pvc, metav1.UpdateOptions{})
		return err
	})
}

func createPVC(c kubernetes.Interface, ns, pvcName, scName string, size int64) error {
	_, err := c.CoreV1().PersistentVolumeClaims(ns).Create(context.Background(), &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: pvcName},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &scName,
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: *resource.NewQuantity(size, resource.BinarySI),
				},
			},
		},
	}, metav1.CreateOptions{})
	return err
}

// ---------------------------------------------------------------------------
// Pod exec helpers
// ---------------------------------------------------------------------------

// execCommandInPod runs cmd inside the first pod matching opt and returns
// stdout/stderr.  It asserts (via Gomega) that the exec call itself succeeds;
// callers only need to inspect the returned strings.
func execCommandInPod(f *framework.Framework, cmd, ns string, opt *metav1.ListOptions) (stdOut, stdErr string) {
	opts := getCommandInPodOpts(f, cmd, ns, opt)
	stdOut, stdErr, err := e2epod.ExecWithOptions(f, opts)
	if stdErr != "" {
		framework.Logf("exec stderr: %v", stdErr)
	}
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "exec %q in pod", cmd)
	return stdOut, stdErr
}

func getCommandInPodOpts(f *framework.Framework, cmd, ns string, opt *metav1.ListOptions) e2epod.ExecOptions {
	podList, err := e2epod.PodClientNS(f, ns).List(context.Background(), *opt)
	framework.ExpectNoError(err, "list pods for exec (selector: %s)", opt.LabelSelector)
	gomega.Expect(podList.Items).NotTo(gomega.BeEmpty(), "no pods matched selector %q", opt.LabelSelector)

	return e2epod.ExecOptions{
		Command:            []string{"/bin/sh", "-c", cmd},
		PodName:            podList.Items[0].Name,
		Namespace:          ns,
		ContainerName:      podList.Items[0].Spec.Containers[0].Name,
		Stdin:              nil,
		CaptureStdout:      true,
		CaptureStderr:      true,
		PreserveWhitespace: true,
	}
}

// writeDataToPod writes data to dataPath inside the first pod matching opt.
func writeDataToPod(f *framework.Framework, opt *metav1.ListOptions, data, dataPath string) {
	execCommandInPod(f, fmt.Sprintf("echo %s > %s", data, dataPath), nameSpace, opt)
}

// compareDataInPod asserts that each data[i] string appears in dataPaths[i]
// inside the first pod matching opt.
func compareDataInPod(f *framework.Framework, opt *metav1.ListOptions, data, dataPaths []string) {
	for i := range data {
		out, _ := execCommandInPod(f, "cat "+dataPaths[i], nameSpace, opt)
		gomega.Expect(out).To(gomega.ContainSubstring(data[i]),
			"data not persisted at path %s", dataPaths[i])
	}
}

// checkDataPersistForMultiPvcs writes distinct content to each of three
// volumes, deletes and recreates the pod, then asserts all data survived the
// restart.
func checkDataPersistForMultiPvcs(f *framework.Framework) {
	dataContents := []string{
		"Data that needs to be stored to vol1",
		"Data that needs to be stored to vol2",
		"Data that needs to be stored to vol3",
	}
	dataPaths := []string{"/spdkvol1/test", "/spdkvol2/test", "/spdkvol3/test"}
	opt := metav1.ListOptions{LabelSelector: "app=spdkcsi-pvc"}

	ginkgo.By("writing data to each volume")
	for i := range dataPaths {
		execCommandInPod(f, fmt.Sprintf("echo %s > %s", dataContents[i], dataPaths[i]), nameSpace, &opt)
	}

	ginkgo.By("deleting and recreating the pod to test persistence")
	deleteTestPodWithMultiPvcs()
	framework.ExpectNoError(waitForTestPodGone(f.ClientSet, multiTestPodName), "wait for multi-PVC pod to terminate")

	deployTestPodWithMultiPvcs()
	framework.ExpectNoError(waitForTestPodReady(f.ClientSet, 3*time.Minute, multiTestPodName), "wait for multi-PVC pod after restart")

	ginkgo.By("verifying data survived the pod restart")
	for i := range dataPaths {
		out, _ := execCommandInPod(f, "cat "+dataPaths[i], nameSpace, &opt)
		gomega.Expect(out).To(gomega.ContainSubstring(dataContents[i]),
			"data not persisted at %s after pod restart", dataPaths[i])
	}
}

// filesystemSizeBytes returns the total capacity (bytes) of the filesystem
// at mountPath as reported by df inside the pod.
func filesystemSizeBytes(f *framework.Framework, opt *metav1.ListOptions, mountPath string) (int64, error) {
	stdOut, stdErr := execCommandInPod(f, fmt.Sprintf("df -P -k %s | awk 'NR==2 {print $2}'", mountPath), nameSpace, opt)
	if stdErr != "" {
		return 0, fmt.Errorf("df stderr: %s", stdErr)
	}
	kib, err := strconv.ParseInt(strings.TrimSpace(stdOut), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse df output %q: %w", stdOut, err)
	}
	return kib * 1024, nil
}

type kubeletStatsSummary struct {
	Pods []struct {
		PodRef struct {
			Name      string `json:"name"`
			Namespace string `json:"namespace"`
			UID       string `json:"uid"`
		} `json:"podRef"`
		VolumeStats []struct {
			Name           string  `json:"name"`
			AvailableBytes *uint64 `json:"availableBytes"`
			CapacityBytes  *uint64 `json:"capacityBytes"`
			UsedBytes      *uint64 `json:"usedBytes"`
		} `json:"volume"`
	} `json:"pods"`
}
