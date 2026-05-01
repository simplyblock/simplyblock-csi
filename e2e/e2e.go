package e2e

import (
	"context"
	"flag"
	"os"
	"path/filepath"
	"testing"
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"k8s.io/klog"
	"k8s.io/kubernetes/test/e2e/framework"
	"k8s.io/kubernetes/test/e2e/framework/config"
	"k8s.io/kubernetes/test/e2e/storage/podlogs"
)

func init() {
	klog.SetOutput(ginkgo.GinkgoWriter)

	if os.Getenv("KUBECONFIG") == "" {
		kubeConfigPath := filepath.Join(os.Getenv("HOME"), ".kube", "config")
		os.Setenv("KUBECONFIG", kubeConfigPath)
	}

	config.CopyFlags(config.Flags, flag.CommandLine)
	framework.RegisterCommonFlags(flag.CommandLine)
	framework.RegisterClusterFlags(flag.CommandLine)

	testing.Init()
	flag.Parse()
	framework.AfterReadingAllFlags(&framework.TestContext)
}

var _ = ginkgo.SynchronizedBeforeSuite(func() []byte {
	if operatorMode {
		ginkgo.By("Waiting for StorageCluster to be active")
		err := waitForStorageClusterActive(30 * time.Minute)
		framework.ExpectNoError(err, "wait for StorageCluster to become active")

		ginkgo.By("Waiting for StorageNodes to be online")
		err = waitForStorageNodeOnline(30 * time.Minute)
		framework.ExpectNoError(err, "wait for StorageNodes to come online")

		ginkgo.By("Waiting for Pool to be ready")
		err = waitForPoolReady(15 * time.Minute)
		framework.ExpectNoError(err, "wait for Pool to become ready")
	}

	ginkgo.By("Watching for pod logs in 'default' namespace")
	cs, err := framework.LoadClientset()
	framework.ExpectNoError(err, "create client set")
	err = podlogs.CopyAllLogs(context.Background(), cs, nameSpace, podlogs.LogOutput{
		LogWriter:    ginkgo.GinkgoWriter,
		StatusWriter: ginkgo.GinkgoWriter,
	})
	framework.ExpectNoError(err, "set watch on namespace :%s", nameSpace)

	return []byte{}
}, func(_ []byte) {})

var _ = ginkgo.SynchronizedAfterSuite(func() {
}, func() {})
