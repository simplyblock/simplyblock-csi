package spdk

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/spdk/spdk-csi/pkg/util"
)

// createSourceVolume provisions a normal volume through the controller and
// returns its CSI volume ID ("{clusterID}:{poolID}:{lvolID}").
func createSourceVolume(t *testing.T, cs *controllerServer, name string) string {
	t.Helper()
	resp, err := cs.CreateVolume(context.Background(), basicCreateVolumeRequest(name))
	if err != nil {
		t.Fatalf("failed to create source volume %q: %v", name, err)
	}
	return resp.GetVolume().GetVolumeId()
}

// assertControlPlaneErrorMapping is the ~27-case-per-RPC response matrix: it drives
// an RPC through every control-plane response (each HTTP status + transport
// failures) by injecting it on the RPC's first API call, and asserts the RPC
// returns the gRPC code its classifier prescribes. This is what verifies the
// error classification is actually wired into the RPC — every response mapped to
// its correct gRPC code, not a blanket Unavailable.
//
//	classify: the RPC's classify*Error function (source of the expected code).
//	invoke:   performs the RPC against the mock and returns its error.
func assertControlPlaneErrorMapping(
	t *testing.T,
	classify func(error) classifiedError,
	invoke func(t *testing.T, ctx context.Context, mock *mockSBCLI) error,
) {
	t.Helper()

	check := func(t *testing.T, repr, actual error) {
		t.Helper()
		if want, got := status.Code(classify(repr)), status.Code(actual); got != want {
			t.Errorf("got code %s, want %s (rpc err: %v)", got, want, actual)
		}
	}

	// Every HTTP status the classifier knows about.
	statuses := []int{
		400, 401, 403, 404, 405, 406, 408, 409, 410, 411, 412, 413, 414, 415, 422, 429,
		500, 501, 502, 503, 504, 505, 507, 508, 511,
	}
	for _, s := range statuses {
		s := s
		t.Run(fmt.Sprintf("http_%d", s), func(t *testing.T) {
			mock := newMockSBCLI()
			defer mock.Close()
			mock.mu.Lock()
			mock.injectStatus = func(*http.Request) int { return s }
			mock.mu.Unlock()
			check(t, &util.HTTPError{StatusCode: s}, invoke(t, context.Background(), mock))
		})
	}

	// Transport failures (no HTTP status).
	t.Run("timeout", func(t *testing.T) {
		mock := newMockSBCLI()
		defer mock.Close()
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		defer cancel()
		check(t, fmt.Errorf("POST: %w", context.DeadlineExceeded), invoke(t, ctx, mock))
	})
	t.Run("connection_refused", func(t *testing.T) {
		mock := newMockSBCLI()
		mock.Close() // dead endpoint; the secret still captures its URL string
		check(t, &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("refused")}, invoke(t, context.Background(), mock))
	})
}

// TestCreateSnapshot_ControlPlaneErrorMapping drives CreateSnapshot through every
// control-plane response and asserts the gRPC code, plus the operation-specific
// 409 idempotency semantics (which are success/AlreadyExists, not a plain code).
func TestCreateSnapshot_ControlPlaneErrorMapping(t *testing.T) {
	const snapName = "snap-map"
	srcVolID := sanityClusterID + ":" + sanityPoolUUID + ":99999999-9999-9999-9999-999999999999"

	assertControlPlaneErrorMapping(t, classifyCreateSnapshotError,
		func(t *testing.T, ctx context.Context, m *mockSBCLI) error {
			cs := newTestControllerServer(t, m)
			_, err := cs.CreateSnapshot(ctx, &csi.CreateSnapshotRequest{SourceVolumeId: srcVolID, Name: snapName})
			return err
		})

	// 409 with the SAME source is our own already-created snapshot -> idempotent
	// success returning the existing snapshot.
	t.Run("http_409_same_source_is_idempotent_success", func(t *testing.T) {
		mock := newMockSBCLI()
		defer mock.Close()
		src := seedSnapshotSource(mock)
		parsed, _ := parseVolumeID(src)
		existingID := uuid.New().String()
		mock.mu.Lock()
		mock.snapshots[existingID] = &mockSnapshot{UUID: existingID, Name: snapName, VolUUID: parsed.lvolID, Size: 1 << 30}
		mock.strictSnapshotNameConflict = true
		mock.mu.Unlock()
		cs := newTestControllerServer(t, mock)

		resp, err := cs.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{SourceVolumeId: src, Name: snapName})
		if err != nil {
			t.Fatalf("409 with the same source must be idempotent success, got: %v", err)
		}
		if id := resp.GetSnapshot().GetSnapshotId(); !strings.Contains(id, existingID) {
			t.Fatalf("expected the existing snapshot %q returned, got %q", existingID, id)
		}
	})

	// 409 with a DIFFERENT source is a genuine name conflict -> AlreadyExists.
	t.Run("http_409_different_source_is_conflict", func(t *testing.T) {
		mock := newMockSBCLI()
		defer mock.Close()
		src := seedSnapshotSource(mock)
		existingID := uuid.New().String()
		mock.mu.Lock()
		mock.snapshots[existingID] = &mockSnapshot{UUID: existingID, Name: snapName, VolUUID: "88888888-8888-8888-8888-888888888888", Size: 1 << 30}
		mock.strictSnapshotNameConflict = true
		mock.mu.Unlock()
		cs := newTestControllerServer(t, mock)

		_, err := cs.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{SourceVolumeId: src, Name: snapName})
		if got := status.Code(err); got != codes.AlreadyExists {
			t.Fatalf("409 with a different source: got %s, want AlreadyExists (err: %v)", got, err)
		}
	})
}

// TestDeleteSnapshot_ControlPlaneErrorMapping drives DeleteSnapshot through every
// control-plane response (notably: 404 must be idempotent success).
func TestDeleteSnapshot_ControlPlaneErrorMapping(t *testing.T) {
	snapID := sanityClusterID + ":" + sanityPoolUUID + ":99999999-9999-9999-9999-999999999999"
	assertControlPlaneErrorMapping(t, classifyDeleteSnapshotError,
		func(t *testing.T, ctx context.Context, m *mockSBCLI) error {
			cs := newTestControllerServer(t, m)
			_, err := cs.DeleteSnapshot(ctx, &csi.DeleteSnapshotRequest{SnapshotId: snapID})
			return err
		})
}

// TestListSnapshots_ControlPlaneErrorMapping drives ListSnapshots through every
// control-plane response.
func TestListSnapshots_ControlPlaneErrorMapping(t *testing.T) {
	assertControlPlaneErrorMapping(t, classifyListSnapshotsError,
		func(t *testing.T, ctx context.Context, m *mockSBCLI) error {
			cs := newTestControllerServer(t, m)
			_, err := cs.ListSnapshots(ctx, &csi.ListSnapshotsRequest{})
			return err
		})
}

// seedSnapshotSource seeds a source volume directly in the mock (bypassing the
// create/GET paths, which the ordering tests deliberately fail) and returns its
// CSI volume ID.
func seedSnapshotSource(m *mockSBCLI) string {
	const srcLvolID = "77777777-7777-7777-7777-777777777777"
	m.mu.Lock()
	m.volumes[srcLvolID] = &mockVolume{UUID: srcLvolID, Name: "pvc-src", Size: 1 << 30}
	m.mu.Unlock()
	return sanityClusterID + ":" + sanityPoolUUID + ":" + srcLvolID
}

// assertNoOrphanOnFailure proves an RPC is atomic on failure: whenever it returns
// an error, its mutating control-plane call (the create) must NOT have persisted.
// Equivalently, the create must be ordered after every OTHER fallible call, so a
// precondition/metadata failure aborts before creating anything — otherwise a
// failure leaves an orphan that later retries can't reconcile.
//
// It is a positional matrix run in two phases:
//
//	Phase 1 — Discovery. Run the RPC once, fully successfully, with a mock whose
//	failIf hook counts every API request but fails none. The count N is the total
//	number of control-plane calls, and the loop bound — so the matrix never loops
//	open-endedly and needs no hardcoded count.
//
//	Phase 2 — Matrix. For each k in 1..N, on a fresh mock, fail ONLY the k-th
//	request. If the RPC then returns an error, the mutation must not have persisted.
//
// A best-effort call whose failure the RPC tolerates (e.g. a post-create info
// read) is fine: the RPC succeeds and the mutation legitimately persists, so the
// no-orphan check only applies when the RPC actually errors. Because N is
// discovered at runtime, any precondition added later gets its own iteration.
//
//	seed:    seeds objects the RPC needs into the mock (nil if none).
//	invoke:  performs the RPC and returns its error.
//	mutated: reports whether the mutating side effect persisted on the mock.
func assertNoOrphanOnFailure(
	t *testing.T,
	seed func(*mockSBCLI),
	invoke func(t *testing.T, ctx context.Context, mock *mockSBCLI) error,
	mutated func(*mockSBCLI) bool,
) {
	t.Helper()
	if seed == nil {
		seed = func(*mockSBCLI) {}
	}

	// Phase 1 — discover the number of API calls a successful RPC makes.
	discover := newMockSBCLI()
	defer discover.Close()
	seed(discover)
	calls := 0
	discover.mu.Lock()
	discover.failIf = func(*http.Request) bool { calls++; return false }
	discover.mu.Unlock()
	if err := invoke(t, context.Background(), discover); err != nil {
		t.Fatalf("discovery invoke failed: %v", err)
	}
	if calls < 2 {
		t.Fatalf("expected the RPC to make at least 2 API calls, got %d", calls)
	}

	// Phase 2 — fail the k-th call; if the RPC errors, no mutation may persist.
	for k := 1; k <= calls; k++ {
		target := k
		t.Run(fmt.Sprintf("fail_call_%d_of_%d", k, calls), func(t *testing.T) {
			mock := newMockSBCLI()
			defer mock.Close()
			seed(mock)
			n := 0
			mock.mu.Lock()
			mock.failIf = func(*http.Request) bool { n++; return n == target }
			mock.mu.Unlock()

			if err := invoke(t, context.Background(), mock); err != nil && mutated(mock) {
				t.Fatalf("API call #%d failed the RPC (%v), yet the mutation persisted (orphan); "+
					"a failed RPC must leave no side effect — create only after every fallible call", target, err)
			}
		})
	}
}

// TestCreateSnapshot_NoAPICallBeforeMetadataResolved: the snapshot must be created
// only after all preconditions/metadata (e.g. the source volume size) resolve.
func TestCreateSnapshot_NoAPICallBeforeMetadataResolved(t *testing.T) {
	const srcLvolID = "77777777-7777-7777-7777-777777777777"
	srcVolID := sanityClusterID + ":" + sanityPoolUUID + ":" + srcLvolID

	assertNoOrphanOnFailure(t,
		func(m *mockSBCLI) {
			m.mu.Lock()
			m.volumes[srcLvolID] = &mockVolume{UUID: srcLvolID, Name: "pvc-src", Size: 1 << 30}
			m.mu.Unlock()
		},
		func(t *testing.T, ctx context.Context, m *mockSBCLI) error {
			cs := newTestControllerServer(t, m)
			_, err := cs.CreateSnapshot(ctx, &csi.CreateSnapshotRequest{SourceVolumeId: srcVolID, Name: "snap-ordering"})
			return err
		},
		func(m *mockSBCLI) bool { m.mu.Lock(); defer m.mu.Unlock(); return len(m.snapshots) > 0 },
	)
}

// TestCreateSnapshot_ReconcilesLeftoverOnRetry is the CreateSnapshot analog of
// TestCreateVolume_ReconcilesLeftoverOnRetry: an ambiguous-timeout leftover must
// be reconciled on retry, not dead-ended on AlreadyExists.
//
// Attempt 1's create is persisted by the control plane but the response is lost,
// so the client sees an error — a valid snapshot of the correct source is left
// behind. On retry the real web API answers 409 for the duplicate name;
// CreateSnapshot must list, see the same source, and return the existing snapshot
// as success (CSI idempotency) — not a duplicate, not AlreadyExists.
func TestCreateSnapshot_ReconcilesLeftoverOnRetry(t *testing.T) {
	mock := newMockSBCLI()
	defer mock.Close()
	mock.mu.Lock()
	mock.strictSnapshotNameConflict = true        // real web API 409s on a duplicate name
	mock.snapshotCreatePersistThenFail = true     // attempt 1: created server-side, response lost
	mock.mu.Unlock()

	cs := newTestControllerServer(t, mock)
	ctx := context.Background()
	srcVolID := seedSnapshotSource(mock)
	req := &csi.CreateSnapshotRequest{SourceVolumeId: srcVolID, Name: "snap-reconcile"}

	// Attempt 1: the snapshot is created but the client sees an error.
	if _, err := cs.CreateSnapshot(ctx, req); err == nil {
		t.Fatal("expected attempt 1 to fail (create persisted but response lost)")
	}
	mock.mu.Lock()
	leftover := len(mock.snapshots)
	mock.mu.Unlock()
	if leftover != 1 {
		t.Fatalf("expected exactly 1 leftover snapshot after attempt 1, got %d", leftover)
	}

	// Attempt 2 (retry, same name + source): must reconcile — return the existing
	// snapshot as success, no duplicate.
	resp, err := cs.CreateSnapshot(ctx, req)
	if err != nil {
		t.Fatalf("retry did not reconcile the leftover snapshot: %v", err)
	}
	if resp.GetSnapshot().GetSnapshotId() == "" {
		t.Fatal("retry returned no snapshot id")
	}
	mock.mu.Lock()
	defer mock.mu.Unlock()
	if got := len(mock.snapshots); got != 1 {
		t.Fatalf("retry created a duplicate: %d snapshots, want 1", got)
	}
}

// TestCreateVolumeFromSnapshot_CloneError_LeavesPVDangling proves that when the
// clone-from-snapshot API call fails, the broken PersistentVolume is left
// dangling — the same cleanup gap as the plain-create path, on the clone path.
//
// handleSnapshotSource returns the raw clone error, which CreateVolume wraps as
// codes.Unavailable and returns. It neither repairs nor cleans up the dangling
// PV left on the Kubernetes side. This test asserts the PV is cleaned up after
// the clone error; it is RED today.
func TestCreateVolumeFromSnapshot_CloneError_LeavesPVDangling(t *testing.T) {
	mock := newMockSBCLI()
	defer mock.Close()

	cs := newTestControllerServer(t, mock)
	ctx := context.Background()

	// A source snapshot the clone will read from.
	srcVolID := createSourceVolume(t, cs, "pvc-clone-source")
	src, err := parseVolumeID(srcVolID)
	if err != nil {
		t.Fatalf("parse source volume ID: %v", err)
	}
	snapID := uuid.New().String()
	mock.mu.Lock()
	mock.snapshots[snapID] = &mockSnapshot{UUID: snapID, Name: "src-snap", VolUUID: src.lvolID, Size: 1 << 30}
	mock.mu.Unlock()
	csiSnapshotID := sanityClusterID + ":" + sanityPoolUUID + ":" + snapID

	const pvName = "pvc-clone-target"
	const orphanLvolID = "88888888-8888-8888-8888-888888888888"

	// The Kubernetes side already holds a broken PV for this clone claim.
	kube := fake.NewSimpleClientset(brokenPV(pvName, orphanLvolID))

	// The control plane fails the clone create.
	mock.failCreateOnce = true
	req := basicCreateVolumeRequest(pvName)
	req.VolumeContentSource = &csi.VolumeContentSource{
		Type: &csi.VolumeContentSource_Snapshot{
			Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: csiSnapshotID},
		},
	}
	if _, err := cs.CreateVolume(ctx, req); err == nil {
		t.Fatal("expected CreateVolume-from-snapshot to fail when the clone API errors")
	}

	pv, err := kube.CoreV1().PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{})
	if err == nil {
		t.Fatalf("failed PV %q was left dangling after the clone API error "+
			"(volume handle %q); CreateVolume neither repaired nor cleaned it up",
			pvName, pv.Spec.CSI.VolumeHandle)
	}
	if !apierrors.IsNotFound(err) {
		t.Fatalf("unexpected error reading PV %q: %v", pvName, err)
	}
}