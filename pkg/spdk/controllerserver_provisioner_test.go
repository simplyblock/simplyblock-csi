package spdk

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	k8sclient "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"

	csicommon "github.com/spdk/spdk-csi/pkg/csi-common"
)

// startCSIController boots the real controller behind a gRPC server (exactly as
// TestSanity does) and returns a CSI ControllerClient dialed to it — so calls
// go PVC-shim -> gRPC -> Simplyblock CSI driver -> mock control plane.
func startCSIController(t *testing.T, mock *mockSBCLI) csi.ControllerClient {
	t.Helper()
	writeMockSecret(t, mock)

	cd := csicommon.NewCSIDriver(testDriverName, "test", "test-node")
	cd.AddControllerServiceCapabilities([]csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
	})
	cd.AddVolumeCapabilityAccessModes([]csi.VolumeCapability_AccessMode_Mode{
		csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
	})

	ids := newIdentityServer(cd)
	cs, err := newControllerServer(cd)
	if err != nil {
		t.Fatalf("newControllerServer: %v", err)
	}
	ns := &stubNodeServer{DefaultNodeServer: csicommon.NewDefaultNodeServer(cd)}

	// Keep the socket path short: unix socket paths are capped (~104 bytes on
	// macOS) and t.TempDir() embeds the long test name.
	sockDir, err := os.MkdirTemp("", "sbcsi")
	if err != nil {
		t.Fatalf("mkdir socket dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(sockDir) })
	endpoint := "unix://" + filepath.Join(sockDir, "c.sock")
	grpcSrv := csicommon.NewNonBlockingGRPCServer()
	grpcSrv.Start(endpoint, ids, cs, ns)
	t.Cleanup(grpcSrv.ForceStop)

	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial controller: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return csi.NewControllerClient(conn)
}

// fakeProvisioner is a minimal stand-in for the Kubernetes external-provisioner
// sidecar: it names the volume pvc-<uid>, calls CreateVolume over gRPC, and on
// success writes a bound PV. On error it leaves the PVC Pending, to be retried —
// exactly the loop that ran 225k times in the incident.
type fakeProvisioner struct {
	client   csi.ControllerClient
	kube     k8sclient.Interface
	scParams map[string]string
}

func (p *fakeProvisioner) provision(ctx context.Context, pvc *corev1.PersistentVolumeClaim) error {
	volName := "pvc-" + string(pvc.UID)
	resp, err := p.client.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name: volName,
		VolumeCapabilities: []*csi.VolumeCapability{{
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
		}},
		CapacityRange: &csi.CapacityRange{RequiredBytes: 1 << 30},
		Parameters:    p.scParams,
	})
	if err != nil {
		return err // PVC stays Pending; the provisioner will retry.
	}

	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: volName},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{Driver: testDriverName, VolumeHandle: resp.GetVolume().GetVolumeId()},
			},
			ClaimRef: &corev1.ObjectReference{Namespace: pvc.Namespace, Name: pvc.Name, UID: pvc.UID},
		},
	}
	if _, err := p.kube.CoreV1().PersistentVolumes().Create(ctx, pv, metav1.CreateOptions{}); err != nil {
		return err
	}
	pvc.Spec.VolumeName = volName
	pvc.Status.Phase = corev1.ClaimBound
	_, err = p.kube.CoreV1().PersistentVolumeClaims(pvc.Namespace).UpdateStatus(ctx, pvc, metav1.UpdateOptions{})
	return err
}

// TestProvisioning_RetryIsIdempotentUnderNameUniqueness drives a PVC through the
// real provisioning path (shim provisioner -> gRPC -> driver -> mock control
// plane) while the post-create publish keeps failing, so the PVC stays Pending
// and the provisioner retries. It asserts that the retries do NOT leak: exactly
// one volume exists on the control plane no matter how many times CreateVolume
// is re-issued.
//
// This idempotency depends on the control plane enforcing volume-name uniqueness
// (409 on a duplicate name): the driver sends a stable name (pvc-<uid>) and
// relies on the 409 to detect and reconcile its own prior volume. A control
// plane that does NOT enforce name-uniqueness would instead create a fresh lvol
// on every retry — one leaked volume per attempt — but that is a control-plane
// contract violation, not a driver bug, so it is out of scope here.
func TestProvisioning_RetryIsIdempotentUnderNameUniqueness(t *testing.T) {
	mock := newMockSBCLI()
	defer mock.Close()
	// Control plane enforces name-uniqueness (default: 409 on duplicate) and the
	// post-create publish (GET) fails, keeping the PVC Pending across retries.
	mock.failGetVolume = true

	ctx := context.Background()
	client := startCSIController(t, mock)
	kube := fake.NewSimpleClientset()

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "data",
			UID:       types.UID("11111111-2222-3333-4444-555555555555"),
		},
	}
	if _, err := kube.CoreV1().PersistentVolumeClaims("default").Create(ctx, pvc, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create pvc: %v", err)
	}

	prov := &fakeProvisioner{
		client:   client,
		kube:     kube,
		scParams: map[string]string{"cluster_id": sanityClusterID, "pool_name": sanityPoolName},
	}

	const retries = 5
	for attempt := 1; attempt <= retries; attempt++ {
		if err := prov.provision(ctx, pvc); err == nil {
			t.Fatalf("attempt %d: provisioning unexpectedly succeeded despite the publish failure", attempt)
		}
	}

	// The PVC never bound (publish kept failing).
	if pvc.Status.Phase == corev1.ClaimBound {
		t.Fatal("PVC unexpectedly bound")
	}

	// No leak: the retries reconciled to the same single volume.
	mock.mu.Lock()
	count := len(mock.volumes)
	mock.mu.Unlock()
	if count != 1 {
		t.Fatalf("expected exactly 1 volume on the control plane after %d retries, got %d "+
			"(retries must reconcile, not leak)", retries, count)
	}
}