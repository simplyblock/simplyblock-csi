package e2e

import (
	"fmt"
	"math/rand"
	"time"

	ginko "github.com/onsi/ginkgo/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/test/e2e/framework"
)

const (
	Size5GB          = 5 * 1024 * 1024 * 1024
	StorageclassName = "spdk-fio-hostid"
	configMapname    = "fio-config"
	pvcName1         = "spdk-fio-pvc1"
	podName1         = "spdk-fio-pod1"
	pvcName2         = "spdk-fio-pvc2"
	podName2         = "spdk-fio-pod2"
)

// TODO: use ctx.Before and ctx.After to cleanup the resources
var _ = ginko.Describe("CSI Driver tests", func() {
	f := framework.NewDefaultFramework("spdkcsi")
	ginko.Context("Control Plane: delete second lvol while the first lvol has IO running", func() {
		ginko.It("if a node has lvol with IO running, adding and deleting an new lvol from the same node should work", func() {
			ginko.By("run a pod with fio and add and delete an lvol from the same node", func() {
				c := f.ClientSet
				r := rand.New(rand.NewSource(time.Now().UnixNano()))
				n, _ := numberOfNodes(c)
				randomValue := r.Intn(n)
				sn, _, err := getStorageNode(c, randomValue)
				if err != nil {
					fmt.Fprintf(ginko.GinkgoWriter, "Error when getStorageNode %s \n", err.Error())
					ginko.Fail(err.Error())
				}

				fmt.Fprintln(ginko.GinkgoWriter, "creating pvc on storage node: ", sn)
				err = createstorageClassWithHostID(c, StorageclassName, sn)
				if err != nil {
					fmt.Fprintf(ginko.GinkgoWriter, "error when creating storage class: %s \n", err.Error())
				}

				err = createFioConfigMap(c, nameSpace, configMapname)
				if err != nil {
					fmt.Fprintf(ginko.GinkgoWriter, "Error when creating Configmap: %s \n", err.Error())
				}

				fmt.Fprintf(ginko.GinkgoWriter, "creating pvc: %s \n", pvcName1)
				err = createPVC(c, nameSpace, pvcName1, StorageclassName, Size5GB)
				if err != nil {
					ginko.Fail(err.Error())
				}

				fmt.Fprintf(ginko.GinkgoWriter, "creating pod: %s \n", podName1)
				err = createFioWorkloadPod(c, nameSpace, podName1, configMapname, pvcName1)
				if err != nil {
					ginko.Fail(err.Error())
				}

				// delete the pod, pvc, storageclass and configmap at the end
				defer func() {
					err2 := c.CoreV1().ConfigMaps(nameSpace).Delete(ctx, configMapname, metav1.DeleteOptions{})
					if err2 != nil {
						fmt.Fprintln(ginko.GinkgoWriter, "Failed to delete configMap: ", err.Error())
						ginko.Fail(err2.Error())
					}
					err2 = c.CoreV1().PersistentVolumeClaims(nameSpace).Delete(ctx, pvcName1, metav1.DeleteOptions{})
					if err2 != nil {
						fmt.Fprintln(ginko.GinkgoWriter, "failed to delete PVC", err.Error())
					}
					err2 = c.CoreV1().Pods(nameSpace).Delete(ctx, podName1, metav1.DeleteOptions{})
					if err2 != nil {
						fmt.Fprintln(ginko.GinkgoWriter, "Failed to delete pod", err.Error())
					}
					err2 = c.StorageV1().StorageClasses().Delete(ctx, StorageclassName, metav1.DeleteOptions{})
					if err2 != nil {
						fmt.Fprintln(ginko.GinkgoWriter, "Failed to delete storageclass", err.Error())
					}
				}()

				fmt.Fprintf(ginko.GinkgoWriter, "creating pvc %s on storage node: %s \n", pvcName2, sn)
				err = createPVC(c, nameSpace, pvcName2, StorageclassName, Size5GB)
				if err != nil {
					ginko.Fail(err.Error())
				}

				fmt.Fprintf(ginko.GinkgoWriter, "creating pod: %s \n", podName2)
				err = createSimplePod(c, nameSpace, podName2, pvcName2)
				if err != nil {
					ginko.Fail(err.Error())
				}

				fmt.Fprintf(ginko.GinkgoWriter, "deleting pod: %s \n", podName2)
				err = c.CoreV1().Pods(nameSpace).Delete(ctx, podName2, metav1.DeleteOptions{})
				if err != nil {
					ginko.Fail(err.Error())
				}

				fmt.Fprintf(ginko.GinkgoWriter, "deleting pvc: %s \n", pvcName2)
				err = c.CoreV1().PersistentVolumeClaims(nameSpace).Delete(ctx, pvcName2, metav1.DeleteOptions{})
				if err != nil {
					ginko.Fail(err.Error())
				}

				// delete the pvc and pod at the end
				// doing this just in case of any failure
				defer func() {
					err2 := c.CoreV1().PersistentVolumeClaims(nameSpace).Delete(ctx, pvcName2, metav1.DeleteOptions{})
					if err2 != nil {
						fmt.Fprintln(ginko.GinkgoWriter, "failed to delete PVC", err.Error())
					}
					err = c.CoreV1().PersistentVolumeClaims(nameSpace).Delete(ctx, pvcName2, metav1.DeleteOptions{})
					if err != nil {
						fmt.Fprintln(ginko.GinkgoWriter, "failed to pod", err.Error())
					}
				}()

				fmt.Fprintf(ginko.GinkgoWriter, "waiting for 10 secs")
				time.Sleep(10 * time.Second)
				// fio should not stop on
				pod, err := c.CoreV1().Pods(nameSpace).Get(ctx, podName1, metav1.GetOptions{})
				if err != nil {
					ginko.Fail(err.Error())
				}

				if pod.Status.Phase != "Running" {
					ginko.Fail("pod is not running")
				} else {
					fmt.Fprintf(ginko.GinkgoWriter, "pod is still running")
				}
			})
		})
	})
})
