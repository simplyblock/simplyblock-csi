package spdk

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	csicommon "github.com/spdk/spdk-csi/pkg/csi-common"
	"github.com/spdk/spdk-csi/pkg/util"
)

const testDriverName = "test.csi.simplyblock.io"

// writeMockSecret writes a SPDKCSI secret pointing at the mock control plane and
// sets the SPDKCSI_SECRET env var to it.
func writeMockSecret(t *testing.T, mock *mockSBCLI) {
	t.Helper()
	secretDir := t.TempDir()
	secretFile := filepath.Join(secretDir, "secret.json")
	secretData, _ := json.Marshal(util.ClustersInfo{
		Clusters: []util.ClusterConfig{{
			ClusterID:       sanityClusterID,
			ClusterEndpoint: mock.URL(),
			ClusterSecret:   sanitySecret,
		}},
	})
	if err := os.WriteFile(secretFile, secretData, 0600); err != nil {
		t.Fatalf("write secret file: %v", err)
	}
	t.Setenv("SPDKCSI_SECRET", secretFile)
}

// newTestControllerServer wires a controllerServer to the given mock control
// plane, mirroring the setup used by TestSanity.
func newTestControllerServer(t *testing.T, mock *mockSBCLI) *controllerServer {
	t.Helper()

	writeMockSecret(t, mock)

	cd := csicommon.NewCSIDriver(testDriverName, "test", "test-node")
	cd.AddControllerServiceCapabilities([]csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
	})
	cd.AddVolumeCapabilityAccessModes([]csi.VolumeCapability_AccessMode_Mode{
		csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
	})

	cs, err := newControllerServer(cd)
	if err != nil {
		t.Fatalf("newControllerServer: %v", err)
	}
	return cs
}

func basicCreateVolumeRequest(name string) *csi.CreateVolumeRequest {
	return &csi.CreateVolumeRequest{
		Name: name,
		VolumeCapabilities: []*csi.VolumeCapability{{
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
		}},
		CapacityRange: &csi.CapacityRange{RequiredBytes: 1 * 1024 * 1024 * 1024},
		Parameters: map[string]string{
			"cluster_id": sanityClusterID,
			"pool_name":  sanityPoolName,
		},
	}
}

// brokenPV builds a PersistentVolume as the external-provisioner would leave
// behind for a dynamically-provisioned claim: the PV name equals the CSI
// volume name, and its volume handle points at an lvol on the control plane.
// Used by teardown-side (DeleteVolume) reproducers where a PV genuinely exists.
func brokenPV(name, lvolID string) *corev1.PersistentVolume {
	return &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					Driver:       testDriverName,
					VolumeHandle: sanityClusterID + ":" + sanityPoolUUID + ":" + lvolID,
				},
			},
		},
	}
}

// volMapID is a well-formed 3-part volume handle (UUID cluster/pool/lvol) whose
// lvol does not exist — the RPC's first API call is injected with each response.
const volMapID = sanityClusterID + ":" + sanityPoolUUID + ":99999999-9999-9999-9999-999999999999"

// TestCreateVolume_ControlPlaneErrorMapping drives CreateVolume through every
// control-plane response and asserts the gRPC code its classifier prescribes.
// A UUID pool_name skips pool resolution, so the first API call is the create POST.
func TestCreateVolume_ControlPlaneErrorMapping(t *testing.T) {
	assertControlPlaneErrorMapping(t, classifyCreateVolumeError,
		func(t *testing.T, ctx context.Context, m *mockSBCLI) error {
			cs := newTestControllerServer(t, m)
			req := basicCreateVolumeRequest("pvc-vol-map")
			req.Parameters["pool_name"] = sanityPoolUUID // UUID → no pool lookup; create POST is first
			_, err := cs.CreateVolume(ctx, req)
			return err
		})
}

// TestDeleteVolume_ControlPlaneErrorMapping drives DeleteVolume through every
// control-plane response (notably: 404 must be idempotent success).
func TestDeleteVolume_ControlPlaneErrorMapping(t *testing.T) {
	assertControlPlaneErrorMapping(t, classifyDeleteVolumeError,
		func(t *testing.T, ctx context.Context, m *mockSBCLI) error {
			cs := newTestControllerServer(t, m)
			_, err := cs.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: volMapID})
			return err
		})
}

// TestControllerExpandVolume_ControlPlaneErrorMapping drives ControllerExpandVolume
// through every control-plane response.
func TestControllerExpandVolume_ControlPlaneErrorMapping(t *testing.T) {
	assertControlPlaneErrorMapping(t, classifyControllerExpandVolumeError,
		func(t *testing.T, ctx context.Context, m *mockSBCLI) error {
			cs := newTestControllerServer(t, m)
			_, err := cs.ControllerExpandVolume(ctx, &csi.ControllerExpandVolumeRequest{
				VolumeId:      volMapID,
				CapacityRange: &csi.CapacityRange{RequiredBytes: 2 << 30},
			})
			return err
		})
}

// TestValidateVolumeCapabilities_ControlPlaneErrorMapping drives
// ValidateVolumeCapabilities through every control-plane response.
func TestValidateVolumeCapabilities_ControlPlaneErrorMapping(t *testing.T) {
	assertControlPlaneErrorMapping(t, classifyValidateVolumeCapabilitiesError,
		func(t *testing.T, ctx context.Context, m *mockSBCLI) error {
			cs := newTestControllerServer(t, m)
			_, err := cs.ValidateVolumeCapabilities(ctx, &csi.ValidateVolumeCapabilitiesRequest{
				VolumeId: volMapID,
				VolumeCapabilities: []*csi.VolumeCapability{{
					AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
				}},
			})
			return err
		})
}

// TestCreateVolume_ReconcilesLeftoverOnRetry proves that CreateVolume's
// create-before-publish ordering is safe *because it reconciles on retry* — as
// long as the control plane dedupes by volume name (409 on a duplicate).
//
// Attempt 1 creates the volume on the control plane but fails at publish, leaving
// a created, online volume behind. The retry (same name) must NOT create a
// duplicate: the ErrVolumeExists list-by-name fallback finds the existing volume,
// re-publishes it (now the control plane is healthy), and returns it. End state:
// exactly one volume, success — no orphan, no duplicate.
//
// Contrast TestProvisioning_APIError_LeaksVolumesWhilePVCStaysPending, where a
// control plane that does NOT dedupe by name makes the same ordering leak one
// volume per retry. So this reconciliation holds only under name-uniqueness.
func TestCreateVolume_ReconcilesLeftoverOnRetry(t *testing.T) {
	mock := newMockSBCLI()
	defer mock.Close()
	cs := newTestControllerServer(t, mock)
	ctx := context.Background()
	req := basicCreateVolumeRequest("pvc-reconcile")

	// Attempt 1: the create succeeds, but publish (a GET) fails → CreateVolume
	// errors, leaving a created online volume behind.
	mock.mu.Lock()
	mock.failGetVolume = true
	mock.mu.Unlock()
	if _, err := cs.CreateVolume(ctx, req); err == nil {
		t.Fatal("expected attempt 1 to fail at publish")
	}
	mock.mu.Lock()
	leftover := len(mock.volumes)
	mock.mu.Unlock()
	if leftover != 1 {
		t.Fatalf("expected exactly 1 leftover volume after attempt 1, got %d", leftover)
	}

	// Attempt 2 (retry, same name): the control plane is healthy again.
	mock.mu.Lock()
	mock.failGetVolume = false
	mock.mu.Unlock()
	resp, err := cs.CreateVolume(ctx, req)
	if err != nil {
		t.Fatalf("retry did not reconcile the leftover volume: %v", err)
	}
	if resp.GetVolume().GetVolumeId() == "" {
		t.Fatal("retry returned no volume id")
	}

	// Exactly one volume must remain — the retry reused the leftover, no duplicate.
	mock.mu.Lock()
	defer mock.mu.Unlock()
	if got := len(mock.volumes); got != 1 {
		t.Fatalf("retry created a duplicate: %d volumes on the control plane, want 1", got)
	}
}

// TestCreateVolume_RetryRecoversFromBrokenVolume proves that CreateVolume gets
// permanently stuck when a previous attempt left a non-online volume on the
// control plane.
//
// Lifecycle: while a PVC is Pending, the external-provisioner calls CreateVolume
// with the SAME name (pvc-<uid>) repeatedly until it succeeds; no PV object
// exists yet. If an earlier attempt created a volume that never came online, the
// retry hits the ErrVolumeExists fallback in createVolume, finds the volume is
// not online, and returns codes.AlreadyExists — every time, forever. The
// provisioner never gets a success, so the PVC never binds.
//
// The retry must instead recover: clean up the broken (non-online) volume and
// create a fresh one, then stay idempotent across further retries. This test
// asserts that; it is RED today.
func TestCreateVolume_RetryRecoversFromBrokenVolume(t *testing.T) {
	mock := newMockSBCLI()
	defer mock.Close()

	cs := newTestControllerServer(t, mock)
	ctx := context.Background()

	const pvName = "pvc-11111111-2222-3333-4444-555555555555"
	const brokenLvolID = "99999999-9999-9999-9999-999999999999"

	// An earlier provisioning attempt left a half-created, non-online volume on
	// the control plane under this name.
	mock.mu.Lock()
	mock.volumes[brokenLvolID] = &mockVolume{
		UUID:   brokenLvolID,
		Name:   pvName,
		Size:   1 << 30,
		Status: "provisioning", // not "online"
	}
	mock.mu.Unlock()

	req := basicCreateVolumeRequest(pvName)

	// The provisioner re-issues the identical request until it succeeds.
	var firstID string
	for attempt := 1; attempt <= 3; attempt++ {
		resp, err := cs.CreateVolume(ctx, req)
		if err != nil {
			t.Fatalf("attempt %d: CreateVolume never recovered from the broken volume; "+
				"it must clean up the non-online volume and create a fresh one. err: %v", attempt, err)
		}
		id := resp.GetVolume().GetVolumeId()
		if id == "" {
			t.Fatalf("attempt %d: CreateVolume returned no volume id", attempt)
		}
		if firstID == "" {
			firstID = id
		} else if id != firstID {
			t.Fatalf("attempt %d: CreateVolume is not idempotent: got %q, first returned %q",
				attempt, id, firstID)
		}
	}

	// Exactly one volume must remain for this name, and it must be online — no
	// orphaned broken volume left behind.
	mock.mu.Lock()
	defer mock.mu.Unlock()
	matches := 0
	for _, v := range mock.volumes {
		if v.Name != pvName {
			continue
		}
		matches++
		if v.status() != "online" {
			t.Errorf("volume %q remained non-online after recovery: status=%q", v.UUID, v.status())
		}
	}
	if matches != 1 {
		t.Errorf("expected exactly one volume named %q after recovery, found %d", pvName, matches)
	}
}
