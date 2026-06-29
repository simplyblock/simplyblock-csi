package kubernetes_test

import (
	"context"
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	sbkube "github.com/spdk/spdk-csi/pkg/kubernetes"
)

const testDriver = "csi.simplyblock.io"

// Realistic handle components: clusterID and lvolID are UUIDs (as production
// volume handles always are); poolID is a name here to exercise that it need
// not be a UUID.
const (
	clusterUUID = "8ffac363-0c46-4714-a71b-f9c0b58a1269"
	poolName    = "pool-1"

	lvolA       = "a1111111-1111-4111-8111-111111111111"
	lvolB       = "b2222222-2222-4222-8222-222222222222"
	lvolC       = "c3333333-3333-4333-8333-333333333333"
	lvolMissing = "d4444444-4444-4444-8444-444444444444"
	lvolZ       = "e5555555-5555-4555-8555-555555555555"
)

func handle(lvolID string) string {
	return clusterUUID + ":" + poolName + ":" + lvolID
}

func testPV(name, driver, handle string) *corev1.PersistentVolume {
	pv := &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: name}}
	if driver != "" || handle != "" {
		pv.Spec.PersistentVolumeSource.CSI = &corev1.CSIPersistentVolumeSource{
			Driver:       driver,
			VolumeHandle: handle,
		}
	}
	return pv
}

func testPVC(namespace, name string) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}}
}

// sampleObjects returns a fresh fixture set on every call so tests never share
// mutable objects through the fake clientset/informer store.
func sampleObjects() []runtime.Object {
	return []runtime.Object{
		testPV("pv-a", testDriver, handle(lvolA)),
		testPV("pv-b", testDriver, handle(lvolB)),
		testPV("pv-other", "other.csi.io", handle(lvolC)),
		testPV("pv-nocsi", "", ""),
		testPVC("ns1", "claim-x"),
	}
}

func nameSet(pvs []*corev1.PersistentVolume) map[string]bool {
	s := make(map[string]bool, len(pvs))
	for _, pv := range pvs {
		s[pv.Name] = true
	}
	return s
}

func waitForSync(t *testing.T, m *sbkube.Manager) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if m.HasSynced() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("cache did not sync within timeout")
}

// assertReads exercises every read method against the standard fixture set and
// is run against both the API-fallback path and the cache path.
func assertReads(t *testing.T, ctx context.Context, m *sbkube.Manager) {
	t.Helper()

	pvs, err := m.PersistentVolumesByDriver(ctx, testDriver)
	if err != nil {
		t.Fatalf("PersistentVolumesByDriver: unexpected error %v", err)
	}
	if got := nameSet(pvs); len(got) != 2 || !got["pv-a"] || !got["pv-b"] {
		t.Fatalf("PersistentVolumesByDriver = %v, want {pv-a, pv-b}", got)
	}

	if pv, err := m.PersistentVolumeByName(ctx, "pv-a"); err != nil || pv == nil || pv.Name != "pv-a" {
		t.Fatalf("PersistentVolumeByName(pv-a) = %v, %v", pv, err)
	}
	if _, err := m.PersistentVolumeByName(ctx, "missing"); !apierrors.IsNotFound(err) {
		t.Fatalf("PersistentVolumeByName(missing) error = %v, want NotFound", err)
	}

	if pv, err := m.PersistentVolumeByLogicalVolumeID(ctx, lvolB); err != nil || pv == nil || pv.Name != "pv-b" {
		t.Fatalf("PersistentVolumeByLogicalVolumeID(lvolB) = %v, %v", pv, err)
	}
	// lvol index is driver-agnostic: a non-simplyblock PV is still found by lvol ID.
	if pv, err := m.PersistentVolumeByLogicalVolumeID(ctx, lvolC); err != nil || pv == nil || pv.Name != "pv-other" {
		t.Fatalf("PersistentVolumeByLogicalVolumeID(lvolC) = %v, %v", pv, err)
	}
	if _, err := m.PersistentVolumeByLogicalVolumeID(ctx, lvolMissing); !apierrors.IsNotFound(err) {
		t.Fatalf("PersistentVolumeByLogicalVolumeID(lvolMissing) error = %v, want NotFound", err)
	}

	if pvc, err := m.PersistentVolumeClaimByNamespaceAndName(ctx, "ns1", "claim-x"); err != nil || pvc == nil || pvc.Name != "claim-x" {
		t.Fatalf("PersistentVolumeClaimByNamespaceAndName(ns1/claim-x) = %v, %v", pvc, err)
	}
	if _, err := m.PersistentVolumeClaimByNamespaceAndName(ctx, "ns1", "missing"); !apierrors.IsNotFound(err) {
		t.Fatalf("PersistentVolumeClaimByNamespaceAndName(missing) error = %v, want NotFound", err)
	}
}

func TestNewManagerNilClient(t *testing.T) {
	if m := sbkube.NewManager(nil); m != nil {
		t.Fatalf("NewManager(nil) = %v, want nil", m)
	}

	var m *sbkube.Manager // nil receiver must be safe
	ctx := context.Background()

	if pvs, err := m.PersistentVolumesByDriver(ctx, testDriver); pvs != nil || err != nil {
		t.Fatalf("nil.PersistentVolumesByDriver = %v, %v; want nil, nil", pvs, err)
	}
	if _, err := m.PersistentVolumeByName(ctx, "x"); !apierrors.IsNotFound(err) {
		t.Fatalf("nil.PersistentVolumeByName error = %v, want NotFound", err)
	}
	if _, err := m.PersistentVolumeByLogicalVolumeID(ctx, "x"); !apierrors.IsNotFound(err) {
		t.Fatalf("nil.PersistentVolumeByLogicalVolumeID error = %v, want NotFound", err)
	}
	if _, err := m.PersistentVolumeClaimByNamespaceAndName(ctx, "ns", "x"); !apierrors.IsNotFound(err) {
		t.Fatalf("nil.PersistentVolumeClaimByNamespaceAndName error = %v, want NotFound", err)
	}
	if m.Client() != nil {
		t.Fatal("nil.Client() != nil")
	}
	if m.HasSynced() {
		t.Fatal("nil.HasSynced() == true")
	}
	m.Start(ctx) // must not panic
}

func TestManagerAPIFallback(t *testing.T) {
	client := fake.NewSimpleClientset(sampleObjects()...)
	m := sbkube.NewManager(client)

	// No Start: the informers never sync, so every read takes the API path.
	if m.HasSynced() {
		t.Fatal("HasSynced() == true before Start")
	}
	assertReads(t, context.Background(), m)
}

func TestManagerServesFromCache(t *testing.T) {
	client := fake.NewSimpleClientset(sampleObjects()...)
	m := sbkube.NewManager(client)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.Start(ctx)
	waitForSync(t, m)

	// Disable the API so any successful read must be served from the cache.
	disabled := errors.New("API disabled")
	react := func(k8stesting.Action) (bool, runtime.Object, error) { return true, nil, disabled }
	client.PrependReactor("list", "*", react)
	client.PrependReactor("get", "*", react)

	assertReads(t, ctx, m)
}

func TestManagerReflectsWatchUpdates(t *testing.T) {
	client := fake.NewSimpleClientset() // start empty
	m := sbkube.NewManager(client)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.Start(ctx)
	waitForSync(t, m)

	if _, err := m.PersistentVolumeByLogicalVolumeID(ctx, lvolZ); !apierrors.IsNotFound(err) {
		t.Fatalf("before create: error = %v, want NotFound", err)
	}

	if _, err := client.CoreV1().PersistentVolumes().Create(ctx,
		testPV("pv-z", testDriver, handle(lvolZ)), metav1.CreateOptions{}); err != nil {
		t.Fatalf("create PV: %v", err)
	}

	// The watch must propagate the new PV into the cache.
	deadline := time.Now().Add(5 * time.Second)
	for {
		pv, err := m.PersistentVolumeByLogicalVolumeID(ctx, lvolZ)
		if err == nil && pv != nil && pv.Name == "pv-z" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("watch update not reflected in cache (last err = %v)", err)
		}
		time.Sleep(5 * time.Millisecond)
	}
}