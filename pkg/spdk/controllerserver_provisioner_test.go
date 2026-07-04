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

// TestProvisioning_APIError_LeaksVolumesWhilePVCStaysPending drives a PVC through
// the real provisioning path (shim provisioner -> gRPC -> driver -> mock control
// plane) and proves the incident behavior: while a control-plane failure keeps
// the PVC Pending, every provisioning retry leaks a fresh volume.
//
// The control plane here does not enforce volume-name uniqueness and fails the
// post-create publish (GET) step, so each CreateVolume creates a new lvol yet
// still returns an error. The driver relies on the control plane for idempotency
// and has no client-side dedup, so volumes pile up one-per-retry — the PVC never
// binds. This test asserts at most one volume survives; it is RED today.
func TestProvisioning_APIError_LeaksVolumesWhilePVCStaysPending(t *testing.T) {
	mock := newMockSBCLI()
	defer mock.Close()
	mock.allowDuplicateNames = true // control plane does not dedupe by name
	mock.failGetVolume = true       // publish (GET) fails after the volume is created

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
			t.Fatalf("attempt %d: provisioning unexpectedly succeeded despite the control-plane error", attempt)
		}
	}

	// The PVC never bound.
	if pvc.Status.Phase == corev1.ClaimBound {
		t.Fatal("PVC unexpectedly bound")
	}

	// The bug: each retry leaked a new volume on the control plane.
	mock.mu.Lock()
	leaked := len(mock.volumes)
	mock.mu.Unlock()
	if leaked > 1 {
		t.Fatalf("control plane leaked %d volumes across %d provisioning retries while the PVC "+
			"stayed Pending; CreateVolume must be idempotent (reuse or clean up the volume) "+
			"instead of creating a new one every attempt", leaked, retries)
	}
}