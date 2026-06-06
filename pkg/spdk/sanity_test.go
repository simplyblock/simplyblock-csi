package spdk

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/kubernetes-csi/csi-test/v5/pkg/sanity"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	csicommon "github.com/spdk/spdk-csi/pkg/csi-common"
	"github.com/spdk/spdk-csi/pkg/util"
)

// ---------------------------------------------------------------------------
// Stub node server — returns success for all node operations so that the
// controller-focused sanity tests can complete their AfterEach cleanups.
// ---------------------------------------------------------------------------

type stubNodeServer struct {
	*csicommon.DefaultNodeServer
}

func (s *stubNodeServer) NodeStageVolume(_ context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	if req.GetStagingTargetPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "staging target path is required")
	}
	if req.GetVolumeCapability() == nil {
		return nil, status.Error(codes.InvalidArgument, "volume capability is required")
	}
	return &csi.NodeStageVolumeResponse{}, nil
}

func (s *stubNodeServer) NodeUnstageVolume(_ context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	if req.GetStagingTargetPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "staging target path is required")
	}
	return &csi.NodeUnstageVolumeResponse{}, nil
}

func (s *stubNodeServer) NodePublishVolume(_ context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	if req.GetTargetPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "target path is required")
	}
	if req.GetVolumeCapability() == nil {
		return nil, status.Error(codes.InvalidArgument, "volume capability is required")
	}
	// Create the target path so the sanity test can verify it exists.
	if tp := req.GetTargetPath(); tp != "" {
		_ = os.MkdirAll(tp, 0750)
	}
	return &csi.NodePublishVolumeResponse{}, nil
}

func (s *stubNodeServer) NodeUnpublishVolume(_ context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	if req.GetTargetPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "target path is required")
	}
	// Remove the target path so the sanity test can verify it was cleaned up.
	if tp := req.GetTargetPath(); tp != "" {
		_ = os.RemoveAll(tp)
	}
	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func (s *stubNodeServer) NodeGetInfo(_ context.Context, _ *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	return &csi.NodeGetInfoResponse{
		NodeId:             "test-node",
		AccessibleTopology: &csi.Topology{},
	}, nil
}

// ---------------------------------------------------------------------------
// Sanity test
// ---------------------------------------------------------------------------

func TestSanity(t *testing.T) {
	mock := newMockSBCLI()
	defer mock.Close()

	// Write secret file pointing at the mock server.
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

	// Build CSI driver + servers (controller only; stub node server for node ops).
	cd := csicommon.NewCSIDriver("test.csi.simplyblock.io", "test", "test-node")
	cd.AddControllerServiceCapabilities([]csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT,
		csi.ControllerServiceCapability_RPC_LIST_SNAPSHOTS,
		csi.ControllerServiceCapability_RPC_EXPAND_VOLUME,
		csi.ControllerServiceCapability_RPC_CLONE_VOLUME,
		csi.ControllerServiceCapability_RPC_GET_VOLUME,
		csi.ControllerServiceCapability_RPC_VOLUME_CONDITION,
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

	// Start gRPC on a Unix socket.
	sockDir := t.TempDir()
	endpoint := "unix://" + filepath.Join(sockDir, "csi.sock")

	grpcSrv := csicommon.NewNonBlockingGRPCServer()
	grpcSrv.Start(endpoint, ids, cs, ns)
	defer grpcSrv.ForceStop()

	// Run the CSI sanity suite.
	sanity.Test(t, sanity.TestConfig{
		Address:     endpoint,
		DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		TestVolumeParameters: map[string]string{
			"cluster_id": sanityClusterID,
			"pool_name":  sanityPoolName,
		},
		TestVolumeSize:       1 * 1024 * 1024 * 1024, // 1 GiB
		TestVolumeAccessType: "mount",
		IDGen:                &sanity.DefaultIDGenerator{},
		TargetPath:           filepath.Join(sockDir, "mount"),
		StagingPath:          filepath.Join(sockDir, "staging"),
	})
}
