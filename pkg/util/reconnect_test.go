// whitebox test of the reconnect PV-ownership gate
package util

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	sbkube "github.com/spdk/spdk-csi/pkg/kubernetes"
)

func pvWithHandle(name, driver, handle string) *corev1.PersistentVolume {
	return &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{Driver: driver, VolumeHandle: handle},
			},
		},
	}
}

func TestIsManagedLvol(t *testing.T) {
	const (
		driver        = "csi.simplyblock.io"
		clusterUUID   = "8ffac363-0c46-4714-a71b-f9c0b58a1269"
		poolName      = "pool-1"
		lvolManaged   = "a1111111-1111-4111-8111-111111111111"
		lvolForeign   = "b2222222-2222-4222-8222-222222222222"
		lvolBenchmark = "c3333333-3333-4333-8333-333333333333"
	)
	handle := func(lvolID string) string { return clusterUUID + ":" + poolName + ":" + lvolID }

	client := fake.NewSimpleClientset(
		pvWithHandle("pv-managed", driver, handle(lvolManaged)),
		pvWithHandle("pv-foreign", "other.csi.io", handle(lvolForeign)),
	)
	// No Start: the manager's API fallback serves these reads deterministically.
	m := sbkube.NewManager(client)

	cases := []struct {
		name string
		lvol string
		want bool
	}{
		{"managed simplyblock volume", lvolManaged, true},
		{"foreign-driver volume with same lvol handle shape", lvolForeign, false},
		{"unknown lvol (e.g. benchmark volume, no PV)", lvolBenchmark, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isManagedLvol(m, tc.lvol, driver); got != tc.want {
				t.Fatalf("isManagedLvol(%q, %q) = %v, want %v", tc.lvol, driver, got, tc.want)
			}
		})
	}
}

// A nil manager (no in-cluster client) must degrade to "not managed" so the
// reconnect loop simply skips ownership-gated work rather than panicking.
func TestIsManagedLvolNilManager(t *testing.T) {
	if isManagedLvol(nil, "lvol-x", "csi.simplyblock.io") {
		t.Fatal("isManagedLvol(nil, ...) = true, want false")
	}
}