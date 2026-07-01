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
	storagev1 "k8s.io/api/storage/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
	"k8s.io/kubernetes/test/e2e/framework"
	e2ekubectl "k8s.io/kubernetes/test/e2e/framework/kubectl"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
)

var (
	// nameSpace is the value from CSI_NAMESPACE env — used only for
	// system-level checks (controller/node readiness) and the global log
	// watcher in e2e.go.  Test helpers accept an explicit ns parameter so
	// each It block is isolated in its own framework-managed namespace.
	nameSpace         string
	storageClassName  string
	snapshotClassName string
	operatorMode      bool
	systemNamespace   string
)

const (
	// Template YAML paths (relative to the e2e/ directory).
	pvcPath                  = "templates/pvc.yaml"
	cachepvcPath             = "templates/pvc-cache.yaml"
	pvcBlockPath             = "templates/pvc-block.yaml"
	testPodPath              = "templates/testpod.yaml"
	cachetestPodPath         = "templates/testpod-cache.yaml"
	testPodBlockPath         = "templates/testpod-block.yaml"
	multiPvcsPath            = "templates/multi-pvc.yaml"
	testPodWithMultiPvcsPath = "templates/testpod-multi-pvc.yaml"
	testPodWithSnapshotPath  = "templates/testpod-snapshot.yaml"
	testPodWithSnapshotPath2 = "templates/testpod-snapshot2.yaml"
	testPodWithClonePath     = "templates/testpod-clone.yaml"
	snapshotOnlyPath         = "templates/snapshot-only.yaml"

	// Kubernetes resource names.
	controllerStsName = "simplyblock-csi-controller"
	nodeDsName        = "simplyblock-csi-node"
	testPodName       = "spdkcsi-test"
	blockTestPodName  = "spdkcsi-test-block"
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
	snapshotClassName = os.Getenv("SNAPSHOT_CLASS_NAME")
	if snapshotClassName == "" {
		snapshotClassName = "simplyblock-csi-snapshotclass"
	}
	operatorMode = os.Getenv("OPERATOR_MODE") == "true"
	systemNamespace = os.Getenv("CSI_SYSTEM_NAMESPACE")
	if systemNamespace == "" {
		systemNamespace = "simplyblock"
	}
}

// newTestFramework creates a Ginkgo e2e framework and registers a BeforeEach
// that labels the framework-managed namespace as pod-security "privileged".
// This is required because framework.NewDefaultFramework creates namespaces
// with the "restricted" PodSecurity profile enforced, which blocks our test
// pods (alpine, running as root, no securityContext).
func newTestFramework(baseName string) *framework.Framework {
	f := framework.NewDefaultFramework(baseName)
	ginkgo.BeforeEach(func() {
		patch := []byte(`{"metadata":{"labels":{` +
			`"pod-security.kubernetes.io/enforce":"privileged",` +
			`"pod-security.kubernetes.io/warn":"privileged",` +
			`"pod-security.kubernetes.io/audit":"privileged"` +
			`}}}`)
		_, err := f.ClientSet.CoreV1().Namespaces().Patch(
			context.Background(),
			f.Namespace.Name,
			types.MergePatchType,
			patch,
			metav1.PatchOptions{},
		)
		framework.ExpectNoError(err, "label namespace %s as pod-security privileged", f.Namespace.Name)
	})
	return f
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
// Deploy helpers — each takes the target namespace as first argument so that
// parallel It blocks are isolated from one another.
// ---------------------------------------------------------------------------

func deployTestPod(ns string) {
	_, err := e2ekubectl.RunKubectl(ns, "apply", "-f", testPodPath)
	framework.ExpectNoError(err, "deploy test pod")
}

func deployPVC(ns string) {
	framework.ExpectNoError(applyTemplateWithStorageClass(ns, pvcPath), "deploy PVC")
}

func deploySnapshot(ns string) {
	framework.ExpectNoError(applyTemplateWithStorageClass(ns, testPodWithSnapshotPath), "deploy snapshot resources")
}

func deploySnapshot2(ns string) {
	framework.ExpectNoError(applyTemplateWithStorageClass(ns, testPodWithSnapshotPath2), "deploy snapshot2 resources")
}

func deployClone(ns string) {
	framework.ExpectNoError(applyTemplateWithStorageClass(ns, testPodWithClonePath), "deploy clone resources")
}

func deployTestPodWithMultiPvcs(ns string) {
	_, err := e2ekubectl.RunKubectl(ns, "apply", "-f", testPodWithMultiPvcsPath)
	framework.ExpectNoError(err, "deploy test pod with multi-PVCs")
}

func deployMultiPvcs(ns string) {
	framework.ExpectNoError(applyTemplateWithStorageClass(ns, multiPvcsPath), "deploy multi-PVCs")
}

// ---------------------------------------------------------------------------
// Delete helpers — best-effort; log but do not fail on error so that a
// cleanup hiccup does not shadow a legitimate test failure.
// ---------------------------------------------------------------------------

func deleteTestPod(ns string) {
	if _, err := e2ekubectl.RunKubectl(ns, "delete", "-f", testPodPath); err != nil {
		framework.Logf("failed to delete test pod: %v", err)
	}
}

func deletePVC(ns string) {
	if _, err := e2ekubectl.RunKubectl(ns, "delete", "-f", pvcPath); err != nil {
		framework.Logf("failed to delete PVC: %v", err)
	}
}

func deleteSnapshot(ns string) {
	if _, err := e2ekubectl.RunKubectl(ns, "delete", "-f", testPodWithSnapshotPath); err != nil {
		framework.Logf("failed to delete snapshot resources: %v", err)
	}
}

func deleteSnapshot2(ns string) {
	if _, err := e2ekubectl.RunKubectl(ns, "delete", "-f", testPodWithSnapshotPath2); err != nil {
		framework.Logf("failed to delete snapshot2 resources: %v", err)
	}
}

func deleteClone(ns string) {
	if _, err := e2ekubectl.RunKubectl(ns, "delete", "-f", testPodWithClonePath); err != nil {
		framework.Logf("failed to delete clone resources: %v", err)
	}
}

func deletePVCAndTestPod(ns string) {
	deleteTestPod(ns)
	deletePVC(ns)
}

func deleteTestPodWithMultiPvcs(ns string) {
	if _, err := e2ekubectl.RunKubectl(ns, "delete", "-f", testPodWithMultiPvcsPath); err != nil {
		framework.Logf("failed to delete multi-PVC test pod: %v", err)
	}
}

func deleteMultiPvcs(ns string) {
	if _, err := e2ekubectl.RunKubectl(ns, "delete", "-f", multiPvcsPath); err != nil {
		framework.Logf("failed to delete multi-PVCs: %v", err)
	}
}

func deleteMultiPvcsAndTestPodWithMultiPvcs(ns string) {
	deleteTestPodWithMultiPvcs(ns)
	deleteMultiPvcs(ns)
}

// ---------------------------------------------------------------------------
// Wait helpers
// ---------------------------------------------------------------------------

// waitForControllerReady and waitForNodeServerReady check system-level
// components, not test resources, so they use the global nameSpace /
// systemNamespace rather than a per-It namespace.
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

// waitForTestPodReady polls until podName in ns is Running with every
// container reporting Ready, or until timeout.  It returns immediately with
// an error if the pod enters a terminal phase (Failed/Succeeded).
func waitForTestPodReady(c kubernetes.Interface, timeout time.Duration, ns, podName string) error {
	err := wait.PollUntilContextTimeout(context.Background(), 3*time.Second, timeout, true,
		func(ctx context.Context) (bool, error) {
			pod, err := c.CoreV1().Pods(ns).Get(ctx, podName, metav1.GetOptions{})
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

func waitForTestPodGone(c kubernetes.Interface, ns, podName string) error {
	err := wait.PollUntilContextTimeout(context.Background(), 3*time.Second, 5*time.Minute, true,
		func(ctx context.Context) (bool, error) {
			_, err := c.CoreV1().Pods(ns).Get(ctx, podName, metav1.GetOptions{})
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

func waitForPvcGone(c kubernetes.Interface, ns, pvcName string) error {
	err := wait.PollUntilContextTimeout(context.Background(), 3*time.Second, 5*time.Minute, true,
		func(ctx context.Context) (bool, error) {
			_, err := c.CoreV1().PersistentVolumeClaims(ns).Get(ctx, pvcName, metav1.GetOptions{})
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

func waitForPVCStorageCapacity(c kubernetes.Interface, ns, pvcName string, minSize resource.Quantity, timeout time.Duration) error {
	err := wait.PollUntilContextTimeout(context.Background(), 3*time.Second, timeout, true,
		func(ctx context.Context) (bool, error) {
			pvc, err := c.CoreV1().PersistentVolumeClaims(ns).Get(ctx, pvcName, metav1.GetOptions{})
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

func waitForFilesystemSize(f *framework.Framework, ns string, opt *metav1.ListOptions, mountPath string, minBytes int64, timeout time.Duration) error {
	err := wait.PollUntilContextTimeout(context.Background(), 5*time.Second, timeout, true,
		func(_ context.Context) (bool, error) {
			sizeBytes, err := filesystemSizeBytes(f, ns, opt, mountPath)
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

func waitForMountedVolumeStats(c kubernetes.Interface, ns, podName string, timeout time.Duration) error {
	err := wait.PollUntilContextTimeout(context.Background(), 10*time.Second, timeout, true,
		func(ctx context.Context) (bool, error) {
			pod, err := c.CoreV1().Pods(ns).Get(ctx, podName, metav1.GetOptions{})
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
				framework.Logf("kubelet stats not yet available on node %s: %v", pod.Spec.NodeName, err)
				return false, nil
			}

			var summary kubeletStatsSummary
			if err := json.Unmarshal(raw, &summary); err != nil {
				return false, fmt.Errorf("parse kubelet stats summary: %w", err)
			}

			for _, podStats := range summary.Pods {
				if podStats.PodRef.Namespace != ns || podStats.PodRef.Name != podName {
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

func resizePVC(c kubernetes.Interface, ns, pvcName string, newSize resource.Quantity) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		pvc, err := c.CoreV1().PersistentVolumeClaims(ns).Get(context.Background(), pvcName, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if pvc.Spec.Resources.Requests == nil {
			pvc.Spec.Resources.Requests = corev1.ResourceList{}
		}
		pvc.Spec.Resources.Requests[corev1.ResourceStorage] = newSize
		_, err = c.CoreV1().PersistentVolumeClaims(ns).Update(context.Background(), pvc, metav1.UpdateOptions{})
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

// execCommandInPod runs cmd inside the first pod matching opt in namespace ns
// and returns stdout/stderr.
func execCommandInPod(f *framework.Framework, cmd, ns string, opt *metav1.ListOptions) (stdOut, stdErr string) {
	opts := getCommandInPodOpts(f, cmd, ns, opt)
	stdOut, stdErr, err := e2epod.ExecWithOptions(f, opts)
	if stdErr != "" {
		framework.Logf("exec stderr: %v", stdErr)
	}
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "exec %q in pod", cmd)
	return stdOut, stdErr
}

// execCommandInPodE is like execCommandInPod but returns the exec error instead
// of failing the test, and targets a Running+Ready pod matching opt (the first
// match otherwise). Use it inside polling assertions where the pod may be
// mid-restart or its volume momentarily unreadable.
func execCommandInPodE(f *framework.Framework, cmd, ns string, opt *metav1.ListOptions) (string, string, error) {
	podList, err := e2epod.PodClientNS(f, ns).List(context.Background(), *opt)
	if err != nil {
		return "", "", err
	}
	if len(podList.Items) == 0 {
		return "", "", fmt.Errorf("no pods matched selector %q", opt.LabelSelector)
	}
	pod := podList.Items[0]
	for i := range podList.Items {
		p := &podList.Items[i]
		if p.Status.Phase == corev1.PodRunning && p.DeletionTimestamp == nil && podReady(p) {
			pod = *p
			break
		}
	}
	return e2epod.ExecWithOptions(f, e2epod.ExecOptions{
		Command:            []string{"/bin/sh", "-c", cmd},
		PodName:            pod.Name,
		Namespace:          ns,
		ContainerName:      pod.Spec.Containers[0].Name,
		CaptureStdout:      true,
		CaptureStderr:      true,
		PreserveWhitespace: true,
	})
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
func writeDataToPod(f *framework.Framework, ns string, opt *metav1.ListOptions, data, dataPath string) {
	execCommandInPod(f, fmt.Sprintf("echo %s > %s", data, dataPath), ns, opt)
}

// compareDataInPod asserts that each data[i] string appears in dataPaths[i].
func compareDataInPod(f *framework.Framework, ns string, opt *metav1.ListOptions, data, dataPaths []string) {
	for i := range data {
		out, _ := execCommandInPod(f, "cat "+dataPaths[i], ns, opt)
		gomega.Expect(out).To(gomega.ContainSubstring(data[i]),
			"data not persisted at path %s", dataPaths[i])
	}
}

// checkDataPersistForMultiPvcs writes distinct content to each of three
// volumes, deletes and recreates the pod, then asserts all data survived.
func checkDataPersistForMultiPvcs(f *framework.Framework, ns string) {
	dataContents := []string{
		"Data that needs to be stored to vol1",
		"Data that needs to be stored to vol2",
		"Data that needs to be stored to vol3",
	}
	dataPaths := []string{"/spdkvol1/test", "/spdkvol2/test", "/spdkvol3/test"}
	opt := metav1.ListOptions{LabelSelector: "app=spdkcsi-pvc"}

	ginkgo.By("writing data to each volume")
	for i := range dataPaths {
		execCommandInPod(f, fmt.Sprintf("echo %s > %s", dataContents[i], dataPaths[i]), ns, &opt)
	}

	ginkgo.By("deleting and recreating the pod to test persistence")
	deleteTestPodWithMultiPvcs(ns)
	framework.ExpectNoError(waitForTestPodGone(f.ClientSet, ns, multiTestPodName), "wait for multi-PVC pod to terminate")

	deployTestPodWithMultiPvcs(ns)
	framework.ExpectNoError(waitForTestPodReady(f.ClientSet, 3*time.Minute, ns, multiTestPodName), "wait for multi-PVC pod after restart")

	ginkgo.By("verifying data survived the pod restart")
	for i := range dataPaths {
		out, _ := execCommandInPod(f, "cat "+dataPaths[i], ns, &opt)
		gomega.Expect(out).To(gomega.ContainSubstring(dataContents[i]),
			"data not persisted at %s after pod restart", dataPaths[i])
	}
}

// filesystemSizeBytes returns the total capacity (bytes) of mountPath via df.
func filesystemSizeBytes(f *framework.Framework, ns string, opt *metav1.ListOptions, mountPath string) (int64, error) {
	stdOut, stdErr := execCommandInPod(f, fmt.Sprintf("df -P -k %s | awk 'NR==2 {print $2}'", mountPath), ns, opt)
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

// ---------------------------------------------------------------------------
// Block-volume helpers
// ---------------------------------------------------------------------------

func deployBlockPVC(ns string) {
	framework.ExpectNoError(applyTemplateWithStorageClass(ns, pvcBlockPath), "deploy block PVC")
}

func deleteBlockPVC(ns string) {
	if _, err := e2ekubectl.RunKubectl(ns, "delete", "-f", pvcBlockPath); err != nil {
		framework.Logf("failed to delete block PVC: %v", err)
	}
}

func deployBlockTestPod(ns string) {
	_, err := e2ekubectl.RunKubectl(ns, "apply", "-f", testPodBlockPath)
	framework.ExpectNoError(err, "deploy block test pod")
}

func deleteBlockTestPod(ns string) {
	if _, err := e2ekubectl.RunKubectl(ns, "delete", "-f", testPodBlockPath); err != nil {
		framework.Logf("failed to delete block test pod: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Snapshot lifecycle helpers (kubectl-based)
// ---------------------------------------------------------------------------

func waitForSnapshotReady(ns, snapshotName string, timeout time.Duration) error {
	err := wait.PollUntilContextTimeout(context.Background(), 5*time.Second, timeout, true,
		func(_ context.Context) (bool, error) {
			out, err := e2ekubectl.RunKubectl(ns, "get", "volumesnapshot", snapshotName,
				"-o", "jsonpath={.status.readyToUse}")
			if err != nil {
				framework.Logf("waiting for snapshot %s to be ready: %v", snapshotName, err)
				return false, nil
			}
			return strings.TrimSpace(out) == "true", nil
		})
	if err != nil {
		return fmt.Errorf("snapshot %q not ready within %s: %w", snapshotName, timeout, err)
	}
	return nil
}

func waitForSnapshotGone(ns, snapshotName string, timeout time.Duration) error {
	err := wait.PollUntilContextTimeout(context.Background(), 3*time.Second, timeout, true,
		func(_ context.Context) (bool, error) {
			out, err := e2ekubectl.RunKubectl(ns, "get", "volumesnapshot", snapshotName,
				"--ignore-not-found=true", "-o", "name")
			if err != nil {
				framework.Logf("checking snapshot %s deletion: %v", snapshotName, err)
				return false, nil
			}
			return strings.TrimSpace(out) == "", nil
		})
	if err != nil {
		return fmt.Errorf("snapshot %q still present after %s: %w", snapshotName, timeout, err)
	}
	return nil
}

func deploySnapshotOnly(ns string) {
	framework.ExpectNoError(applyTemplateWithStorageClass(ns, snapshotOnlyPath), "deploy snapshot-only resource")
}

func deleteSnapshotOnly(ns string) {
	if _, err := e2ekubectl.RunKubectl(ns, "delete", "-f", snapshotOnlyPath); err != nil {
		framework.Logf("failed to delete snapshot-only resource: %v", err)
	}
}

// ---------------------------------------------------------------------------
// PV helpers
// ---------------------------------------------------------------------------

// waitForPVDeleted polls until the named PersistentVolume is gone.
func waitForPVDeleted(c kubernetes.Interface, pvName string, timeout time.Duration) error {
	err := wait.PollUntilContextTimeout(context.Background(), 3*time.Second, timeout, true,
		func(ctx context.Context) (bool, error) {
			_, err := c.CoreV1().PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{})
			if k8serrors.IsNotFound(err) {
				return true, nil
			}
			return false, err
		})
	if err != nil {
		return fmt.Errorf("PV %q not deleted within %s: %w", pvName, timeout, err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// StorageClass helpers
// ---------------------------------------------------------------------------

func createStorageClassWithParams(c kubernetes.Interface, scName string, extraParams map[string]string) {
	createStorageClassWithParamsAndLabels(c, scName, extraParams, nil)
}

// createStorageClassWithParamsAndLabels is like createStorageClassWithParams but
// also sets metadata labels on the StorageClass (e.g. the guardian
// auto-restart-on-pathloss opt-in).
func createStorageClassWithParamsAndLabels(c kubernetes.Interface, scName string, extraParams, scLabels map[string]string) {
	base, err := c.StorageV1().StorageClasses().Get(context.Background(), storageClassName, metav1.GetOptions{})
	if err != nil {
		ginkgo.Skip(fmt.Sprintf("base StorageClass %q unavailable (%v) — skipping", storageClassName, err))
		return
	}

	params := make(map[string]string, len(base.Parameters)+len(extraParams))
	for k, v := range base.Parameters {
		params[k] = v
	}
	for k, v := range extraParams {
		params[k] = v
	}

	sc := &storagev1.StorageClass{
		ObjectMeta:           metav1.ObjectMeta{Name: scName, Labels: scLabels},
		Provisioner:          base.Provisioner,
		Parameters:           params,
		ReclaimPolicy:        base.ReclaimPolicy,
		VolumeBindingMode:    base.VolumeBindingMode,
		AllowVolumeExpansion: base.AllowVolumeExpansion,
		AllowedTopologies:    base.AllowedTopologies,
	}
	_, err = c.StorageV1().StorageClasses().Create(context.Background(), sc, metav1.CreateOptions{})
	framework.ExpectNoError(err, "create StorageClass %s", scName)
}

// liveClusterID resolves the UUID of the simplyblock cluster the tests run
// against from `sbctl cluster list` (matched by CLUSTER_NAME when set, else the
// sole cluster). Tests use this to build StorageClasses pinned to the current
// cluster instead of relying on a possibly stale operator-created SC.
func liveClusterID(f *framework.Framework) string {
	name := os.Getenv("CLUSTER_NAME")
	id := sbctlClusterID(f, name)
	gomega.Expect(id).NotTo(gomega.BeEmpty(),
		"resolve live cluster UUID (CLUSTER_NAME=%q) from sbctl cluster list", name)
	return id
}

// createFilesystemTestPod creates a single alpine pod that mounts pvcName as a
// filesystem at /spdkvol, labelled app=appLabel. It mirrors templates/testpod.yaml
// but lets callers point the PVC at a test-owned StorageClass.
func createFilesystemTestPod(c kubernetes.Interface, ns, podName, appLabel, pvcName string) error {
	_, err := c.CoreV1().Pods(ns).Create(context.Background(), &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: podName, Labels: map[string]string{"app": appLabel}},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:         "alpine",
				Image:        "alpine:3",
				Command:      []string{"sleep", "365d"},
				VolumeMounts: []corev1.VolumeMount{{Name: "spdk-volume", MountPath: "/spdkvol"}},
			}},
			Volumes: []corev1.Volume{{
				Name: "spdk-volume",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvcName},
				},
			}},
		},
	}, metav1.CreateOptions{})
	return err
}

func deleteStorageClass(c kubernetes.Interface, scName string) {
	if err := c.StorageV1().StorageClasses().Delete(context.Background(), scName, metav1.DeleteOptions{}); err != nil {
		framework.Logf("failed to delete StorageClass %s: %v", scName, err)
	}
}

// ---------------------------------------------------------------------------
// PVC annotation helper
// ---------------------------------------------------------------------------

func createAnnotatedPVC(c kubernetes.Interface, ns, pvcName, scName string, size resource.Quantity, annotations map[string]string) error {
	_, err := c.CoreV1().PersistentVolumeClaims(ns).Create(context.Background(), &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        pvcName,
			Annotations: annotations,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &scName,
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: size},
			},
		},
	}, metav1.CreateOptions{})
	return err
}

// ---------------------------------------------------------------------------
// Negative-test helpers
// ---------------------------------------------------------------------------

// assertPVCStaysPending polls the named PVC for duration and fails if it
// ever leaves the Pending phase.
func assertPVCStaysPending(c kubernetes.Interface, ns, pvcName string, duration time.Duration) {
	deadline := time.Now().Add(duration)
	for time.Now().Before(deadline) {
		pvc, err := c.CoreV1().PersistentVolumeClaims(ns).Get(context.Background(), pvcName, metav1.GetOptions{})
		framework.ExpectNoError(err, "get PVC %s while asserting Pending", pvcName)
		gomega.Expect(pvc.Status.Phase).To(gomega.Equal(corev1.ClaimPending),
			"PVC %s should stay Pending (current phase: %s)", pvcName, pvc.Status.Phase)
		time.Sleep(3 * time.Second)
	}
}

// createPodForPVC creates a minimal alpine pod that mounts pvcName at /spdkvol.
func createPodForPVC(c kubernetes.Interface, ns, podName, pvcName string) error {
	_, err := c.CoreV1().Pods(ns).Create(context.Background(), &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:   podName,
			Labels: map[string]string{"app": podName},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:    "alpine",
				Image:   "alpine:3",
				Command: []string{"sleep", "365d"},
				VolumeMounts: []corev1.VolumeMount{{
					Name:      "vol",
					MountPath: "/spdkvol",
				}},
			}},
			Volumes: []corev1.Volume{{
				Name: "vol",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: pvcName,
					},
				},
			}},
		},
	}, metav1.CreateOptions{})
	return err
}

// deletePodByName deletes a pod by name; logs but does not fail on error.
func deletePodByName(c kubernetes.Interface, ns, podName string) {
	if err := c.CoreV1().Pods(ns).Delete(context.Background(), podName, metav1.DeleteOptions{}); err != nil {
		framework.Logf("failed to delete pod %s: %v", podName, err)
	}
}

// ---------------------------------------------------------------------------
// Reconnect helpers — shared by the SPDKCSI-RECONNECT-* specs.
// ---------------------------------------------------------------------------

// poolNameForTests resolves the storage pool the reconnect tests should use for
// directly-created (unmanaged) volumes. It prefers E2E_SB_POOL, then the
// pool_name of the StorageClass under test (guaranteed to exist on this
// cluster), then POOL_NAME. It never returns "" — as a last resort it falls back
// to "testing1" so misconfiguration surfaces as a clear "pool not found".
func poolNameForTests(c kubernetes.Interface) string {
	if p := os.Getenv("E2E_SB_POOL"); p != "" {
		return p
	}
	if sc, err := c.StorageV1().StorageClasses().Get(context.Background(), storageClassName, metav1.GetOptions{}); err == nil {
		if p := sc.Parameters["pool_name"]; p != "" {
			return p
		}
	}
	if p := os.Getenv("POOL_NAME"); p != "" {
		return p
	}
	return "testing1"
}

// managedWorkload is a node-pinned workload and the NVMe-oF volume it consumes,
// set up for a total-path-loss scenario. It is produced by setupManagedWorkload
// and shared by SPDKCSI-RECONNECT-FULLLOSS and SPDKCSI-RECONNECT-GUARDIAN.
type managedWorkload struct {
	ns              string
	appLabel        string
	mode            fullLossMode
	pod             *corev1.Pod
	workerNode      string
	pluginPod       string
	pluginContainer string
	lvolID          string
	sub             *nvmeSubsystem
}

// setupManagedWorkload provisions everything a total-path-loss test needs: a
// StorageClass on the live cluster that opts in to guardian auto-restart, a PVC
// and a node-pinned pod for mode m, a marker written to the volume, and the
// volume's NVMe-oF subsystem located on the node. It registers DeferCleanups for
// the StorageClass and Deployment and returns the handles needed to drive and
// verify recovery.
//
// We build our own StorageClass (pinned to the live cluster_id via sbctl) rather
// than reusing the operator's default SC, which may reference a stale cluster.
func setupManagedWorkload(f *framework.Framework, m fullLossMode, appLabel string) managedWorkload {
	ns := f.Namespace.Name
	pvcName := appLabel + "-pvc"

	ginkgo.By("check the node DaemonSet is ready")
	framework.ExpectNoError(waitForNodeServerReady(f.ClientSet, 3*time.Minute), "node DaemonSet ready")

	ginkgo.By("create an opt-in StorageClass on the live cluster and the PVC")
	scName := fmt.Sprintf("%s-%s", appLabel, ns)
	// max_namespace_per_subsys=1 gives each volume its own NVMe-oF subsystem, so
	// the NQN carries this volume's lvol id (not a shared subsystem's), a total
	// disconnect affects only this volume (no cross-spec interference under
	// parallelism), and staging never races a sibling's "already connected" path.
	scParams := map[string]string{
		"cluster_id":               liveClusterID(f),
		"max_namespace_per_subsys": "1",
	}
	if !m.block {
		scParams["csi.storage.k8s.io/fstype"] = m.fsType
	}
	createStorageClassWithParamsAndLabels(f.ClientSet, scName, scParams,
		map[string]string{"simplyblock.io/auto-restart-on-pathloss": "true"})
	ginkgo.DeferCleanup(func() { deleteStorageClass(f.ClientSet, scName) })
	framework.ExpectNoError(createModePVC(f.ClientSet, ns, pvcName, scName, m.block), "create PVC")

	ginkgo.By("pick a worker node and run a pod pinned to it")
	workerNode, _, _ := anyNodePluginPod(f.ClientSet)
	framework.ExpectNoError(
		createPinnedDeployment(f.ClientSet, ns, appLabel, appLabel, pvcName, workerNode, m.block),
		"create workload")
	ginkgo.DeferCleanup(func() {
		_ = f.ClientSet.AppsV1().Deployments(ns).Delete(context.Background(), appLabel, metav1.DeleteOptions{})
	})
	pod := waitForReadyPod(f.ClientSet, ns, appLabel, "", 5*time.Minute)

	ginkgo.By("write a marker to the volume")
	marker := appLabel + "-" + ns
	if len(marker) > 60 {
		marker = marker[:60]
	}
	writeMarker(f, ns, appLabel, m, marker)

	ginkgo.By("locate the csi-node pod and the volume's NVMe subsystem")
	pluginPod, pluginContainer := nodePluginPodOnNode(f.ClientSet, workerNode)
	lvolID := lvolIDForPVC(f.ClientSet, ns, pvcName)
	sub := waitForSubsystem(f, pluginPod, pluginContainer, lvolID, time.Minute)

	return managedWorkload{
		ns:              ns,
		appLabel:        appLabel,
		mode:            m,
		pod:             pod,
		workerNode:      workerNode,
		pluginPod:       pluginPod,
		pluginContainer: pluginContainer,
		lvolID:          lvolID,
		sub:             sub,
	}
}

// induceTotalPathLoss disconnects the whole NVMe-oF subsystem for the workload's
// volume on its node, so the kernel removes the device (total path loss) and the
// in-place mount goes dead.
func (w managedWorkload) induceTotalPathLoss(f *framework.Framework) {
	ginkgo.By("induce TOTAL path loss by disconnecting the whole subsystem")
	execInPod(f, driverNamespace(), w.pluginPod, w.pluginContainer, "nvme disconnect -n "+w.sub.NQN)
}
