package e2e

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	. "github.com/onsi/gomega" //nolint
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	e2ekubectl "k8s.io/kubernetes/test/e2e/framework/kubectl"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"

	"github.com/spdk/spdk-csi/pkg/util"
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

func deployCacheTestPod() {
	_, err := e2ekubectl.RunKubectl(nameSpace, "apply", "-f", cachetestPodPath)
	if err != nil {
		framework.Logf("failed to create cache test pod: %s", err)
	}
}

func deleteCacheTestPod() {
	_, err := e2ekubectl.RunKubectl(nameSpace, "delete", "-f", cachetestPodPath)
	if err != nil {
		framework.Logf("failed to delete cache test pod: %s", err)
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

func deployCachePVC() {
	if err := applyTemplateWithStorageClass(nameSpace, cachepvcPath); err != nil {
		framework.Logf("failed to create cache pvc: %s", err)
	}
}

func deleteCachePVC() {
	_, err := e2ekubectl.RunKubectl(nameSpace, "delete", "-f", cachepvcPath)
	if err != nil {
		framework.Logf("failed to delete cache pvc: %s", err)
	}
}

func deleteCachePVCAndCacheTestPod() {
	deleteCacheTestPod()
	deleteCachePVC()
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

func waitForCacheTestPodReady(c kubernetes.Interface, timeout time.Duration) error {
	err := wait.PollImmediate(3*time.Second, timeout, func() (bool, error) {
		pod, err := c.CoreV1().Pods(nameSpace).Get(ctx, cachetestPodName, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		if string(pod.Status.Phase) == PodStatusRunning {
			return true, nil
		}
		return false, nil
	})
	if err != nil {
		return fmt.Errorf("failed to wait for cache test pod ready: %w", err)
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

func checkDataPersist(f *framework.Framework) error {
	data := "Data that needs to be stored"
	// write data to PVC
	dataPath := "/spdkvol/test"
	opt := metav1.ListOptions{
		LabelSelector: "app=spdkcsi-pvc",
	}
	execCommandInPod(f, fmt.Sprintf("echo %s > %s", data, dataPath), nameSpace, &opt)

	deleteTestPod()
	err := waitForTestPodGone(f.ClientSet, testPodName)
	if err != nil {
		return err
	}

	deployTestPod()
	err = waitForTestPodReady(f.ClientSet, 5*time.Minute, testPodName)
	if err != nil {
		return err
	}

	// read data from PVC
	persistData, stdErr := execCommandInPod(f, "cat "+dataPath, nameSpace, &opt)
	Expect(stdErr).Should(BeEmpty()) //nolint
	if !strings.Contains(persistData, data) {
		return fmt.Errorf("data not persistent: expected data %s received data %s ", data, persistData)
	}

	return err
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

func verifyDynamicPVCreation(c kubernetes.Interface, pvcName string, timeout time.Duration) error {
	err := wait.PollImmediate(3*time.Second, timeout, func() (bool, error) {
		pvc, err := c.CoreV1().PersistentVolumeClaims(nameSpace).Get(ctx, pvcName, metav1.GetOptions{})
		if err != nil {
			return false, err
		}

		if pvc.Status.Phase != corev1.ClaimBound {
			return false, nil
		}

		pvName := pvc.Spec.VolumeName
		pv, err := c.CoreV1().PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{})
		if err != nil {
			return false, err
		}

		return pv.Spec.ClaimRef != nil && pv.Spec.StorageClassName != "", nil
	})
	if err != nil {
		return fmt.Errorf("failed to verify dynamic PV creation for PVC %s: %w", pvcName, err)
	}
	return nil
}

type simplyblockCreds struct {
	Simplyblock SimplyBlock `json:"simplybk"`
}

type SimplyBlock struct {
	IP     string `json:"ip"`
	UUID   string `json:"uuid"`
	Secret string `json:"secret"`
}

type csiClusterEntry struct {
	ClusterID       string `json:"cluster_id"`
	ClusterEndpoint string `json:"cluster_endpoint"`
	ClusterSecret   string `json:"cluster_secret"`
}

type csiCredentialsV2 struct {
	Clusters []csiClusterEntry `json:"clusters"`
}

// getSimplyBlockCreds returns cluster credentials from the appropriate source.
// In operator mode it reads simplyblock-csi-secret-v2 from systemNamespace;
// otherwise it reads simplyblock-csi-cm + simplyblock-csi-secret from nameSpace.
func getSimplyBlockCreds(c kubernetes.Interface) (SimplyBlock, error) {
	if operatorMode {
		secret, err := c.CoreV1().Secrets(systemNamespace).Get(ctx, "simplyblock-csi-secret-v2", metav1.GetOptions{})
		if err != nil {
			return SimplyBlock{}, err
		}
		var creds csiCredentialsV2
		if err := json.Unmarshal(secret.Data["secret.json"], &creds); err != nil {
			return SimplyBlock{}, err
		}
		if len(creds.Clusters) == 0 {
			return SimplyBlock{}, errors.New("no clusters found in simplyblock-csi-secret-v2")
		}
		c0 := creds.Clusters[0]
		return SimplyBlock{IP: c0.ClusterEndpoint, UUID: c0.ClusterID, Secret: c0.ClusterSecret}, nil
	}

	cm, err := c.CoreV1().ConfigMaps(nameSpace).Get(ctx, "simplyblock-csi-cm", metav1.GetOptions{})
	if err != nil {
		return SimplyBlock{}, err
	}
	var creds simplyblockCreds
	if err := json.Unmarshal([]byte(cm.Data["config.json"]), &creds); err != nil {
		return SimplyBlock{}, err
	}
	secret, err := c.CoreV1().Secrets(nameSpace).Get(ctx, "simplyblock-csi-secret", metav1.GetOptions{})
	if err != nil {
		return SimplyBlock{}, err
	}
	if err := json.Unmarshal(secret.Data["secret.json"], &creds); err != nil {
		return SimplyBlock{}, err
	}
	return creds.Simplyblock, nil
}

type StorageNodes struct {
	Nodes []StorageNode `json:"results"`
}

type StorageNode struct {
	UUID        string `json:"id"`
	APIendpoint string `json:"api_endpoint"`
}

func (s SimplyBlock) getStoragenode(random int) (string, string, error) {
	var rpcClient util.RPCClient
	rpcClient.ClusterID = s.UUID
	rpcClient.ClusterIP = s.IP
	rpcClient.ClusterSecret = s.Secret

	rpcClient.HTTPClient = &http.Client{Timeout: 10 * time.Second}

	// get the list of storage nodes
	out, err := rpcClient.CallSBCLI("GET", "/storagenode", nil)
	if err != nil {
		return "", "", err
	}

	// TODO: get a random storage node
	storageNodes, ok := out.([]interface{})[random].(map[string]interface{})

	if !ok {
		return "", "", errors.New("failed to get storage node from simplyblock api")
	}
	sn, ok := storageNodes["hostname"].(string)
	snid, ok := storageNodes["uuid"].(string)

	if !ok {
		return "", "", errors.New("failed to get storage node from simplyblock api")
	}
	return sn, snid, nil
}

func (s SimplyBlock) numberOfNodes() (int, error) {
	var rpcClient util.RPCClient
	rpcClient.ClusterID = s.UUID
	rpcClient.ClusterIP = s.IP
	rpcClient.ClusterSecret = s.Secret

	rpcClient.HTTPClient = &http.Client{Timeout: 10 * time.Second}

	out, err := rpcClient.CallSBCLI("GET", "/storagenode", nil)
	if err != nil {
		return 0, err
	}

	//get the number of storage nodes
	sn := len(out.([]interface{}))
	return sn, nil

}

func checkNodeStatus(nodeID string, expected string, rpcClient util.RPCClient, retries int, delay time.Duration) error {
	for try := 1; try <= retries; try++ {
		time.Sleep(delay)

		url := fmt.Sprintf("/storagenode/%s", nodeID)
		response, err := rpcClient.CallSBCLI("GET", url, nil)
		if err != nil {
			return fmt.Errorf("error calling RPC: %w", err)
		}

		respArray, ok := response.([]interface{})
		if !ok || len(respArray) == 0 {
			return fmt.Errorf("unexpected response format: %v", response)
		}

		resp, ok := respArray[0].(map[string]interface{})
		if !ok {
			return fmt.Errorf("unexpected response format: %v", respArray[0])
		}

		status, ok := resp["status"].(string)
		if !ok {
			return fmt.Errorf("status field missing or invalid in response: %v", resp)
		}

		// check node is online and healthy
		if expected == "online" {
			healthy, ok := resp["health_check"].(bool)
			if !ok {
				return fmt.Errorf("health field missing or invalid in response: %v", resp)
			}
			if status == expected && healthy {
				return nil
			}
		}

		if status == expected {
			return nil
		}
	}

	return fmt.Errorf("storage node %s did not transition to '%s' state after %d retries", nodeID, expected, retries)
}

func (s SimplyBlock) restartStorageNode(nodeID string) error {

	var rpcClient util.RPCClient
	rpcClient.ClusterID = s.UUID
	rpcClient.ClusterIP = s.IP
	rpcClient.ClusterSecret = s.Secret
	rpcClient.HTTPClient = http.DefaultClient

	// Step 1: Suspend Storage Node
	url := fmt.Sprintf("/storagenode/suspend/%s", nodeID)
	if _, err := rpcClient.CallSBCLI("GET", url, nil); err != nil {
		return fmt.Errorf("failed to suspend storage node: %w", err)
	}
	//check whether the node has suspended
	expectedStatus := "suspended"
	retries := 20
	delay := 10 * time.Second

	err := checkNodeStatus(nodeID, expectedStatus, rpcClient, retries, delay)

	if err != nil {
		return err
	}

	// Step 2: Shutdown Storage Node
	url = fmt.Sprintf("/storagenode/shutdown/%s/?force=True", nodeID)
	if _, err := rpcClient.CallSBCLI("GET", url, nil); err != nil {
		return fmt.Errorf("failed to shutdown storage node: %w", err)
	}

	//check whether the node has shutdown
	expectedStatus = "offline"
	err = checkNodeStatus(nodeID, expectedStatus, rpcClient, retries, delay)

	if err != nil {
		return err
	}

	// Step 3: Fetch Storage Node Info
	url = fmt.Sprintf("/storagenode/%s", nodeID)
	resp, err := rpcClient.CallSBCLI("GET", url, nil)
	if err != nil {
		return fmt.Errorf("failed to fetch storage node info: %w", err)
	}

	result, ok := resp.([]interface{})[0].(map[string]interface{})
	if !ok {
		return fmt.Errorf("type assertion failed")
	}

	// Step 4: Restart Storage Node
	args := storageNode{
		UUID:   result["id"].(string),
		NodeIP: result["api_endpoint"].(string),
	}
	url = "/storagenode/restart/"
	if _, err := rpcClient.CallSBCLI("PUT", url, args); err != nil {
		return fmt.Errorf("failed to restart storage node: %w", err)
	}

	expectedStatus = "online"
	err = checkNodeStatus(nodeID, expectedStatus, rpcClient, retries, delay)

	if err != nil {
		return err
	} else {
		return nil
	}

}

func waitForPodRunning(ctx context.Context, c kubernetes.Interface, namespace, podName string, timeout time.Duration) error {
	// Create a timeout context
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Polling interval
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for pod %s to be running", podName)
		case <-ticker.C:
			pod, err := c.CoreV1().Pods(namespace).Get(ctx, podName, metav1.GetOptions{})
			if err != nil {
				return fmt.Errorf("failed to get pod %s: %w", podName, err)
			}
			if pod.Status.Phase == PodStatusRunning {
				return nil
			}
			// Optionally, handle other statuses, e.g., Failed or Unknown
			// fmt.Printf("Current status of pod %s is %s\n", podName, pod.Status.Phase)
		}
	}
}

func createSimplePod(c kubernetes.Interface, nameSpace, podName, pvcClaimName string) error {
	volumeName := "spdk-csi-vol"
	_, err := c.CoreV1().Pods(nameSpace).Create(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: podName,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  "spdk-csi-container",
					Image: "busybox:latest",
					Command: []string{
						"sleep",
						"100000",
					},
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      volumeName,
							MountPath: "/spdkvol",
						},
					},
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: volumeName,
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: pvcClaimName,
						},
					},
				},
			},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		return err
	}

	// wait for the pod to be running
	return waitForPodRunning(ctx, c, nameSpace, podName, 5*time.Minute)
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

func createFioWorkloadPod(c kubernetes.Interface, nameSpace, podName, configMapName, pvcClaimName string) error {
	// create a pod with the storage class
	// RUN fio workload on this pod
	volumeName := "spdk-csi-vol"
	_, err := c.CoreV1().Pods(nameSpace).Create(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: podName,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  "spdk-csi-container",
					Image: "manoharbrm/fio:latest",
					Command: []string{
						"fio",
						"/fio/fio.cfg",
					},
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      volumeName,
							MountPath: "/spdkvol",
						},
						{
							Name:      configMapName,
							MountPath: "/fio",
						},
					},
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: volumeName,
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: pvcClaimName,
						},
					},
				},
				{
					Name: configMapName,
					VolumeSource: corev1.VolumeSource{
						ConfigMap: &corev1.ConfigMapVolumeSource{
							LocalObjectReference: corev1.LocalObjectReference{
								Name: configMapName,
							},
						},
					},
				},
			},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		return err
	}
	err = waitForPodRunning(ctx, c, nameSpace, podName, 1*time.Minute)
	if err != nil {
		return err
	}
	return nil
}

func createFioConfigMap(c kubernetes.Interface, nameSpace, configMapName string) error {
	_, err := c.CoreV1().ConfigMaps(nameSpace).Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: configMapName,
		},
		Data: map[string]string{
			"fio.cfg": `
				[test]
				ioengine=aiolib
				direct=1
				iodepth=4
				time_based=1
				runtime=1000
				readwrite=randrw
				bs=4K,8K,16K,32K,64K,128K,256K
				nrfiles=4
				size=4G
				verify=md5
				numjobs=3
				directory=/spdkvol`,
		},
	}, metav1.CreateOptions{})
	if err != nil {
		return err
	}
	return nil
}

func createstorageClassWithHostID(c kubernetes.Interface, storageClassName, hostID string) error {
	allowVolumeExpansion := true
	storageClass := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: storageClassName,
		},
		Provisioner: "csi.simplyblock.io",
		Parameters: map[string]string{
			"hostID":                    hostID,
			"pool_name":                 "testing1",
			"distr_ndcs":                "1",
			"distr_npcs":                "1",
			"qos_rw_iops":               "0",
			"qos_rw_mbytes":             "0",
			"qos_r_mbytes":              "0",
			"qos_w_mbytes":              "0",
			"compression":               "False",
			"encryption":                "False",
			"csi.storage.k8s.io/fstype": "ext4",
		},
		AllowVolumeExpansion: &allowVolumeExpansion,
	}

	_, err := c.StorageV1().StorageClasses().Create(ctx, storageClass, metav1.CreateOptions{})
	return err
}

func getStorageNode(c kubernetes.Interface, random int) (string, string, error) {
	s, err := getSimplyBlockCreds(c)
	if err != nil {
		return "", "", err
	}
	return s.getStoragenode(random)
}
func numberOfNodes(c kubernetes.Interface) (int, error) {
	s, err := getSimplyBlockCreds(c)
	if err != nil {
		return 0, err
	}
	return s.numberOfNodes()
}

func restartStorageNode(c kubernetes.Interface, nodeID string) error {
	s, err := getSimplyBlockCreds(c)
	if err != nil {
		return err
	}
	return s.restartStorageNode(nodeID)
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
