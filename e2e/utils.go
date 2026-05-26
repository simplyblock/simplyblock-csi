package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/gomega" //nolint
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

var nameSpace string
var storageClassName string
var operatorMode bool
var systemNamespace string


const (

	// deployment yaml files
	yamlDir                  = "../deploy/kubernetes/"
	driverPath               = yamlDir + "driver.yaml"
	secretPath               = yamlDir + "secret.yaml"
	configmapPath            = yamlDir + "config-map.yaml"
	nodeserverConfigmapPath  = yamlDir + "nodeserver-config-map.yaml"
	controllerRbacPath       = yamlDir + "controller-rbac.yaml"
	nodeRbacPath             = yamlDir + "node-rbac.yaml"
	controllerPath           = yamlDir + "controller.yaml"
	nodePath                 = yamlDir + "node.yaml"
	storageClassPath         = yamlDir + "storageclass.yaml"
	cachingnodePath          = yamlDir + "caching-node.yaml"
	jobPath                  = yamlDir + "job.yaml"
	pvcPath                  = "templates/pvc.yaml"
	cachepvcPath             = "templates/pvc-cache.yaml"
	testPodPath              = "templates/testpod.yaml"
	cachetestPodPath         = "templates/testpod-cache.yaml"
	multiPvcsPath            = "templates/multi-pvc.yaml"
	testPodWithMultiPvcsPath = "templates/testpod-multi-pvc.yaml"
	testPodWithSnapshotPath  = "templates/testpod-snapshot.yaml"
	testPodWithSnapshotPath2 = "templates/testpod-snapshot2.yaml"
	testPodWithClonePath     = "templates/testpod-clone.yaml"

	// controller statefulset and node daemonset names
	controllerStsName = "simplyblock-csi-controller"
	nodeDsName        = "simplyblock-csi-node"
	testPodName       = "spdkcsi-test"
	multiTestPodName  = "spdkcsi-test-multi"
	cachetestPodName  = "spdkcsi-cache-test"
	PodStatusRunning  = "Running"
)

var ctx = context.TODO()

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

func deployTestPod() {
	_, err := e2ekubectl.RunKubectl(nameSpace, "apply", "-f", testPodPath)
	if err != nil {
		framework.Logf("failed to create test pod: %s", err)
	}
}

func deleteTestPod() {
	_, err := e2ekubectl.RunKubectl(nameSpace, "delete", "-f", testPodPath)
	if err != nil {
		framework.Logf("failed to delete test pod: %s", err)
	}
}

func deployPVC() {
	if err := applyTemplateWithStorageClass(nameSpace, pvcPath); err != nil {
		framework.Logf("failed to create pvc: %s", err)
	}
}

func deletePVC() {
	_, err := e2ekubectl.RunKubectl(nameSpace, "delete", "-f", pvcPath)
	if err != nil {
		framework.Logf("failed to delete pvc: %s", err)
	}
}

func deploySnapshot() {
	if err := applyTemplateWithStorageClass(nameSpace, testPodWithSnapshotPath); err != nil {
		framework.Logf("failed to deployed snapshot: %s", err)
	}
}

func deleteSnapshot() {
	_, err := e2ekubectl.RunKubectl(nameSpace, "delete", "-f", testPodWithSnapshotPath)
	if err != nil {
		framework.Logf("failed to delete snapshot: %s", err)
	}
}

func deploySnapshot2() {
	if err := applyTemplateWithStorageClass(nameSpace, testPodWithSnapshotPath2); err != nil {
		framework.Logf("failed to deployed snapshot: %s", err)
	}
}

func deleteSnapshot2() {
	_, err := e2ekubectl.RunKubectl(nameSpace, "delete", "-f", testPodWithSnapshotPath2)
	if err != nil {
		framework.Logf("failed to delete snapshot: %s", err)
	}
}

func deployClone() {
	if err := applyTemplateWithStorageClass(nameSpace, testPodWithClonePath); err != nil {
		framework.Logf("failed to deployed Cloned Volume: %s", err)
	}
}

func deleteClone() {
	_, err := e2ekubectl.RunKubectl(nameSpace, "delete", "-f", testPodWithClonePath)
	if err != nil {
		framework.Logf("failed to delete cloned volume : %s", err)
	}
}

func deletePVCAndTestPod() {
	deleteTestPod()
	deletePVC()
}

func deployTestPodWithMultiPvcs() {
	_, err := e2ekubectl.RunKubectl(nameSpace, "apply", "-f", testPodWithMultiPvcsPath)
	if err != nil {
		framework.Logf("failed to create test pod with multiple pvcs: %s", err)
	}
}

func deleteTestPodWithMultiPvcs() {
	_, err := e2ekubectl.RunKubectl(nameSpace, "delete", "-f", testPodWithMultiPvcsPath)
	if err != nil {
		framework.Logf("failed to delete test pod with multiple pvcs: %s", err)
	}
}

func deployMultiPvcs() {
	if err := applyTemplateWithStorageClass(nameSpace, multiPvcsPath); err != nil {
		framework.Logf("failed to create pvcs: %s", err)
	}
}

func deleteMultiPvcs() {
	_, err := e2ekubectl.RunKubectl(nameSpace, "delete", "-f", multiPvcsPath)
	if err != nil {
		framework.Logf("failed to delete pvcs: %s", err)
	}
}

func deleteMultiPvcsAndTestPodWithMultiPvcs() {
	deleteTestPodWithMultiPvcs()
	deleteMultiPvcs()
}

func waitForControllerReady(c kubernetes.Interface, timeout time.Duration) error {
	ns := nameSpace
	if operatorMode {
		ns = systemNamespace
	}
	err := wait.PollImmediate(3*time.Second, timeout, func() (bool, error) {
		sts, err := c.AppsV1().StatefulSets(ns).Get(ctx, controllerStsName, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		if sts.Status.Replicas == sts.Status.ReadyReplicas {
			return true, nil
		}
		return false, nil
	})
	if err != nil {
		return fmt.Errorf("failed to wait for controller ready: %w", err)
	}
	return nil
}

func waitForNodeServerReady(c kubernetes.Interface, timeout time.Duration) error {
	ns := nameSpace
	if operatorMode {
		ns = systemNamespace
	}
	err := wait.PollImmediate(3*time.Second, timeout, func() (bool, error) {
		ds, err := c.AppsV1().DaemonSets(ns).Get(ctx, nodeDsName, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		if ds.Status.NumberReady == ds.Status.DesiredNumberScheduled {
			return true, nil
		}
		return false, nil
	})
	if err != nil {
		return fmt.Errorf("failed to wait for node server ready: %w", err)
	}
	return nil
}

func waitForTestPodReady(c kubernetes.Interface, timeout time.Duration, testPodName string) error {
	err := wait.PollImmediate(3*time.Second, timeout, func() (bool, error) {
		pod, err := c.CoreV1().Pods(nameSpace).Get(ctx, testPodName, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		if string(pod.Status.Phase) == PodStatusRunning {
			return true, nil
		}
		return false, nil
	})
	if err != nil {
		return fmt.Errorf("failed to wait for test pod ready: %w", err)
	}
	return nil
}

func waitForTestPodGone(c kubernetes.Interface, testPodName string) error {
	err := wait.PollImmediate(3*time.Second, 5*time.Minute, func() (bool, error) {
		_, err := c.CoreV1().Pods(nameSpace).Get(ctx, testPodName, metav1.GetOptions{})
		if err != nil {
			if k8serrors.IsNotFound(err) {
				return true, nil
			}
			return false, err
		}
		return false, nil
	})
	if err != nil {
		return fmt.Errorf("failed to wait for test pod gone: %w", err)
	}
	return nil
}

func waitForPvcGone(c kubernetes.Interface, pvcName string) error {
	err := wait.PollImmediate(3*time.Second, 5*time.Minute, func() (bool, error) {
		_, err := c.CoreV1().PersistentVolumeClaims(nameSpace).Get(ctx, pvcName, metav1.GetOptions{})
		if err != nil {
			if k8serrors.IsNotFound(err) {
				return true, nil
			}
			return false, err
		}
		return false, nil
	})
	if err != nil {
		return fmt.Errorf("failed to wait for pvc (%s) gone: %w", pvcName, err)
	}
	return nil
}

func execCommandInPod(f *framework.Framework, c, ns string, opt *metav1.ListOptions) (stdOut, stdErr string) {
	podPot := getCommandInPodOpts(f, c, ns, opt)
	stdOut, stdErr, err := e2epod.ExecWithOptions(f, podPot)
	if stdErr != "" {
		framework.Logf("stdErr occurred: %v", stdErr)
	}
	Expect(err).ShouldNot(HaveOccurred()) //nolint
	return stdOut, stdErr
}

func getCommandInPodOpts(f *framework.Framework, c, ns string, opt *metav1.ListOptions) e2epod.ExecOptions {
	cmd := []string{"/bin/sh", "-c", c}
	podList, err := e2epod.PodClientNS(f, ns).List(ctx, *opt)
	framework.ExpectNoError(err)
	Expect(podList.Items).NotTo(BeNil())  //nolint
	Expect(err).ShouldNot(HaveOccurred()) //nolint

	return e2epod.ExecOptions{
		Command:            cmd,
		PodName:            podList.Items[0].Name,
		Namespace:          ns,
		ContainerName:      podList.Items[0].Spec.Containers[0].Name,
		Stdin:              nil,
		CaptureStdout:      true,
		CaptureStderr:      true,
		PreserveWhitespace: true,
	}
}

func checkDataPersistForMultiPvcs(f *framework.Framework) error {
	dataContents := []string{
		"Data that needs to be stored to vol1",
		"Data that needs to be stored to vol2",
		"Data that needs to be stored to vol3",
	}
	// write data to PVC
	dataPaths := []string{
		"/spdkvol1/test",
		"/spdkvol2/test",
		"/spdkvol3/test",
	}
	opt := metav1.ListOptions{
		LabelSelector: "app=spdkcsi-pvc",
	}
	for i := 0; i < len(dataPaths); i++ {
		execCommandInPod(f, fmt.Sprintf("echo %s > %s", dataContents[i], dataPaths[i]), nameSpace, &opt)
	}

	deleteTestPodWithMultiPvcs()
	err := waitForTestPodGone(f.ClientSet, multiTestPodName)
	if err != nil {
		return err
	}

	deployTestPodWithMultiPvcs()
	err = waitForTestPodReady(f.ClientSet, 3*time.Minute, multiTestPodName)
	if err != nil {
		return err
	}

	// read data from PVC
	for i := 0; i < len(dataPaths); i++ {
		persistData, stdErr := execCommandInPod(f, "cat "+dataPaths[i], nameSpace, &opt)
		Expect(stdErr).Should(BeEmpty()) //nolint
		if !strings.Contains(persistData, dataContents[i]) {
			return fmt.Errorf("data not persistent: expected data %s received data %s ", dataContents[i], persistData)
		}
	}
	return err
}

func resizePVC(c kubernetes.Interface, pvcName string, newSize resource.Quantity) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		pvc, err := c.CoreV1().PersistentVolumeClaims(nameSpace).Get(ctx, pvcName, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if pvc.Spec.Resources.Requests == nil {
			pvc.Spec.Resources.Requests = corev1.ResourceList{}
		}
		pvc.Spec.Resources.Requests[corev1.ResourceStorage] = newSize
		_, err = c.CoreV1().PersistentVolumeClaims(nameSpace).Update(ctx, pvc, metav1.UpdateOptions{})
		return err
	})
}

func waitForPVCStorageCapacity(c kubernetes.Interface, pvcName string, minSize resource.Quantity, timeout time.Duration) error {
	err := wait.PollImmediate(3*time.Second, timeout, func() (bool, error) {
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
		return fmt.Errorf("failed to wait for PVC %s capacity to reach %s: %w", pvcName, minSize.String(), err)
	}
	return nil
}

func waitForFilesystemSize(f *framework.Framework, opt *metav1.ListOptions, mountPath string, minBytes int64, timeout time.Duration) error {
	err := wait.PollImmediate(5*time.Second, timeout, func() (bool, error) {
		sizeBytes, err := filesystemSizeBytes(f, opt, mountPath)
		if err != nil {
			framework.Logf("failed to read filesystem size: %v", err)
			return false, err
		}
		return sizeBytes >= minBytes, nil
	})
	if err != nil {
		return fmt.Errorf("failed to wait for filesystem at %s to reach at least %d bytes: %w", mountPath, minBytes, err)
	}
	return nil
}

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

func waitForMountedVolumeStats(c kubernetes.Interface, podName string, timeout time.Duration) error {
	err := wait.PollImmediate(10*time.Second, timeout, func() (bool, error) {
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
			framework.Logf("failed to read kubelet stats summary from node %s: %v", pod.Spec.NodeName, err)
			return false, fmt.Errorf("failed to read kubelet stats summary from node %s: %w", pod.Spec.NodeName, err)
		}

		var summary kubeletStatsSummary
		if err := json.Unmarshal(raw, &summary); err != nil {
			return false, fmt.Errorf("failed to parse kubelet stats summary: %w", err)
		}

		for _, podStats := range summary.Pods {
			if podStats.PodRef.Namespace != nameSpace || podStats.PodRef.Name != podName {
				continue
			}
			for _, volume := range podStats.VolumeStats {
				if volume.CapacityBytes != nil && *volume.CapacityBytes > 0 &&
					volume.AvailableBytes != nil &&
					volume.UsedBytes != nil {
					return true, nil
				}
			}
			return false, nil
		}

		return false, nil
	})
	if err != nil {
		return fmt.Errorf("failed to wait for kubelet volume stats for pod %s: %w", podName, err)
	}
	return nil
}

func createPVC(c kubernetes.Interface, nameSpace, pvcName, storageClassName string, size int64) error {
	_, err := c.CoreV1().PersistentVolumeClaims(nameSpace).Create(ctx, &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: pvcName,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &storageClassName,
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: *resource.NewQuantity(size, resource.BinarySI), // 256Mi
				},
			},
		},
	}, metav1.CreateOptions{})
	return err
}


func writeDataToPod(f *framework.Framework, opt *metav1.ListOptions, data, dataPath string) {
	execCommandInPod(f, fmt.Sprintf("echo %s > %s", data, dataPath), nameSpace, opt)
}

func compareDataInPod(f *framework.Framework, opt *metav1.ListOptions, data, dataPaths []string) error {
	for i := range data {
		// read data from PVC
		persistData, stdErr := execCommandInPod(f, "cat "+dataPaths[i], nameSpace, opt)
		Expect(stdErr).Should(BeEmpty()) //nolint
		if !strings.Contains(persistData, data[i]) {
			return fmt.Errorf("data not persistent: expected data %s received data %s ", data[i], persistData)
		}
	}
	return nil
}
