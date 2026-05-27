package e2e

import (
	"flag"
	"os"
	"path/filepath"
	"testing"

	ginkgo "github.com/onsi/ginkgo/v2"
	"k8s.io/klog"
	"k8s.io/kubernetes/test/e2e/framework"
	"k8s.io/kubernetes/test/e2e/framework/config"
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
	klog.InitFlags(flag.CommandLine)

	testing.Init()
	flag.Parse()
	framework.AfterReadingAllFlags(&framework.TestContext)
}

var _ = ginkgo.SynchronizedBeforeSuite(func() []byte {
	return []byte{}
}, func(_ []byte) {})

var _ = ginkgo.SynchronizedAfterSuite(func() {
}, func() {})
