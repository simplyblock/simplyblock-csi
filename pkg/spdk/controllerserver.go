/*
Copyright (c) Arm Limited and Contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package spdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/spdk/spdk-csi/pkg/kubernetes/volumehandle"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/klog"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	csicommon "github.com/spdk/spdk-csi/pkg/csi-common"
	"github.com/spdk/spdk-csi/pkg/util"
)

// var errVolumeInCreation = status.Error(codes.Internal, "volume in creation")
const (
	CSIStorageBaseKey      = "csi.storage.k8s.io/pvc"
	CSIStorageNameKey      = CSIStorageBaseKey + "/name"
	CSIStorageNamespaceKey = CSIStorageBaseKey + "/namespace"

	annotationNvmfModelID = "simplyblock.io/nvmf-model-id"
	annotationLvolID      = "simplyblock.io/lvol-id"
	annotationHostID      = "simplyblock.io/host-id"
	annotationQoSRWIOPS   = "simplyblock.io/qos-rw-iops"
	annotationQoSRWMBps   = "simplyblock.io/qos-rw-mbps"
	annotationQoSRMBps    = "simplyblock.io/qos-r-mbps"
	annotationQoSWMBps    = "simplyblock.io/qos-w-mbps"

	// Deprecated annotation keys — still supported for backward compatibility.
	deprecatedAnnotationNvmfModelID = "simplybk/nvmf-model-id"
	deprecatedAnnotationLvolID      = "simplybk/lvol-id"
	deprecatedAnnotationHostID      = "simplybk/host-id"
	deprecatedAnnotationQoSRWIOPS   = "simplybk/qos-rw-iops"
	deprecatedAnnotationQoSRWMBps   = "simplybk/qos-rw-mbytes"
	deprecatedAnnotationQoSRMBps    = "simplybk/qos-r-mbytes"
	deprecatedAnnotationQoSWMBps    = "simplybk/qos-w-mbytes"

	paramClusterID          = "cluster_id"
	paramZoneClusterMap     = "zone_cluster_map"
	paramRegionClusterMap   = "region_cluster_map"
	topologyKeyZoneStable   = "topology.kubernetes.io/zone"
	topologyKeyZoneBeta     = "failure-domain.beta.kubernetes.io/zone"
	topologyKeyRegionStable = "topology.kubernetes.io/region"
)

type controllerServer struct {
	*csicommon.DefaultControllerServer
	volumeLocks *util.VolumeLocks
}

type spdkVolume struct {
	clusterID string
	lvolID    string
	poolID    string
}

type spdkSnapshot struct {
	clusterID  string
	poolID     string
	snapshotID string
}

type clusterSelection struct {
	clusterID string
	topology  map[string]string
}

func (cs *controllerServer) resolveClusterSelection(req *csi.CreateVolumeRequest) (*clusterSelection, error) {
	params := req.GetParameters()
	if params == nil {
		return nil, fmt.Errorf("missing parameters in CreateVolumeRequest")
	}

	if id, ok := params[paramClusterID]; ok {
		id = strings.TrimSpace(id)
		if id != "" {
			return &clusterSelection{clusterID: id}, nil
		}
	}

	var (
		zoneMap   map[string]string
		regionMap map[string]string
		err       error
	)

	if raw, ok := params[paramZoneClusterMap]; ok {
		zoneMap, err = parseStringMap(raw, paramZoneClusterMap)
		if err != nil {
			return nil, err
		}
	}

	if raw, ok := params[paramRegionClusterMap]; ok {
		regionMap, err = parseStringMap(raw, paramRegionClusterMap)
		if err != nil {
			return nil, err
		}
	}

	if len(zoneMap) == 0 && len(regionMap) == 0 {
		return nil, fmt.Errorf("no %s or %s provided and %s not set",
			paramZoneClusterMap, paramRegionClusterMap, paramClusterID)
	}

	topoReq := req.GetAccessibilityRequirements()

	tryList := func(list []*csi.Topology) *clusterSelection {
		for _, topo := range list {
			if sel := matchTopologyWithZoneMap(topo, zoneMap); sel != nil {
				return sel
			}
			if sel := matchTopologyWithRegionMap(topo, regionMap); sel != nil {
				return sel
			}
		}
		return nil
	}

	if topoReq != nil {
		if sel := tryList(topoReq.GetPreferred()); sel != nil {
			return sel, nil
		}
		if sel := tryList(topoReq.GetRequisite()); sel != nil {
			return sel, nil
		}
		// Topology was provided but contains no zone or region key recognised by
		// the StorageClass map. This means the worker node is missing the required
		// topology labels.
		nodeName := nodeNameFromTopology(topoReq.GetPreferred())
		if nodeName == "" {
			nodeName = nodeNameFromTopology(topoReq.GetRequisite())
		}
		return nil, fmt.Errorf(
			"node %q has no %s or %s topology label but the StorageClass uses %s/%s "+
				"for cluster routing; add the appropriate zone or region label to the node, "+
				"or switch the StorageClass to use %s",
			nodeName, topologyKeyZoneStable, topologyKeyRegionStable,
			paramZoneClusterMap, paramRegionClusterMap, paramClusterID,
		)
	}

	return nil, fmt.Errorf(
		"no topology requirements received; the StorageClass uses %s or %s for cluster "+
			"routing but the node has no topology labels — add %s or %s labels to the node, "+
			"or switch the StorageClass to use %s",
		paramZoneClusterMap, paramRegionClusterMap,
		topologyKeyZoneStable, topologyKeyRegionStable, paramClusterID,
	)
}

func parseStringMap(raw, paramName string) (map[string]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("%s parameter is empty", paramName)
	}

	var parsed map[string]string
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		return nil, fmt.Errorf("failed to parse %s parameter: %w", paramName, err)
	}

	normalized := make(map[string]string, len(parsed))
	for key, value := range parsed {
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key == "" || value == "" {
			continue
		}
		normalized[key] = value
	}

	if len(normalized) == 0 {
		return nil, fmt.Errorf("%s parameter did not contain any mappings", paramName)
	}
	return normalized, nil
}

// nodeNameFromTopology extracts the node name from the simplyblock hostname
// fallback topology key set by the node plugin when no zone/region labels exist.
func nodeNameFromTopology(topos []*csi.Topology) string {
	for _, topo := range topos {
		if topo == nil {
			continue
		}
		if name, ok := topo.GetSegments()["topology.simplyblock.io/hostname"]; ok && name != "" {
			return name
		}
	}
	return "unknown"
}

func matchTopologyWithRegionMap(topo *csi.Topology, regionMap map[string]string) *clusterSelection {
	if topo == nil {
		return nil
	}

	region := regionFromTopology(topo)
	if region == "" {
		return nil
	}

	clusterID, ok := regionMap[region]
	if !ok {
		return nil
	}

	clusterID = strings.TrimSpace(clusterID)
	if clusterID == "" {
		return nil
	}

	return &clusterSelection{
		clusterID: clusterID,
		topology:  map[string]string{topologyKeyRegionStable: region},
	}
}

func matchTopologyWithZoneMap(topo *csi.Topology, zoneMap map[string]string) *clusterSelection {
	if topo == nil {
		return nil
	}

	zone := zoneFromTopology(topo)
	if zone == "" {
		return nil
	}

	clusterID, ok := zoneMap[zone]
	if !ok {
		return nil
	}

	clusterID = strings.TrimSpace(clusterID)
	if clusterID == "" {
		return nil
	}

	return &clusterSelection{
		clusterID: clusterID,
		topology:  copyTopologySegments(topo.GetSegments()),
	}
}

func zoneFromTopology(topo *csi.Topology) string {
	if topo == nil {
		return ""
	}
	return zoneFromSegments(topo.GetSegments())
}

func zoneFromSegments(segments map[string]string) string {
	if segments == nil {
		return ""
	}
	if zone, ok := segments[topologyKeyZoneStable]; ok && zone != "" {
		return zone
	}
	if zone, ok := segments[topologyKeyZoneBeta]; ok && zone != "" {
		return zone
	}
	return ""
}

func regionFromSegments(segments map[string]string) string {
	if segments == nil {
		return ""
	}
	if r, ok := segments[topologyKeyRegionStable]; ok && r != "" {
		return r
	}
	return ""
}

func regionFromTopology(topo *csi.Topology) string {
	if topo == nil {
		return ""
	}
	return regionFromSegments(topo.GetSegments())
}

func copyTopologySegments(segments map[string]string) map[string]string {
	if len(segments) == 0 {
		return nil
	}

	copied := make(map[string]string, len(segments))
	for k, v := range segments {
		copied[k] = v
	}
	return copied
}

// CreateVolume creates a new volume in the SimplyBlock storage system.
func (cs *controllerServer) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume name is required")
	}
	if len(req.GetVolumeCapabilities()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "volume capabilities are required")
	}

	volumeID := req.GetName()
	unlock := cs.volumeLocks.Lock(volumeID)
	defer unlock()

	selection, err := cs.resolveClusterSelection(req)
	if err != nil {
		klog.Errorf("failed to resolve cluster selection for volume %s: %v", volumeID, err)
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	poolName := req.GetParameters()["pool_name"]
	sbClient, err := util.NewsimplyBlockClient(ctx, selection.clusterID, poolName)
	if err != nil {
		return nil, err
	}

	csiVolume, err := cs.createVolume(ctx, req, sbClient)
	if err != nil {
		klog.Errorf("failed to create volume, volumeID: %s err: %v", volumeID, err)
		if _, isStatus := status.FromError(err); isStatus {
			return nil, err
		}
		return nil, classifyCreateVolumeError(err)
	}

	volumeInfo, err := cs.publishVolume(ctx, csiVolume.GetVolumeId(), sbClient)
	if err != nil {
		klog.Errorf("failed to publish volume, volumeID: %s err: %v", volumeID, err)
		return nil, classifyCreateVolumeError(err)
	}

	// copy volume info. node needs these info to contact target(ip, port, nqn, ...)
	if csiVolume.VolumeContext == nil {
		csiVolume.VolumeContext = volumeInfo
	} else {
		for k, v := range volumeInfo {
			csiVolume.VolumeContext[k] = v
		}
	}

	if csiVolume.VolumeContext == nil {
		csiVolume.VolumeContext = map[string]string{}
	}
	csiVolume.VolumeContext[paramClusterID] = selection.clusterID

	if volType, ok := req.GetParameters()["type"]; ok {
		csiVolume.VolumeContext["targetType"] = volType
	}

	if selection.topology != nil {
		csiVolume.AccessibleTopology = []*csi.Topology{{Segments: copyTopologySegments(selection.topology)}}

		if zone := zoneFromSegments(selection.topology); zone != "" {
			csiVolume.VolumeContext[topologyKeyZoneStable] = zone
		}
		if region := regionFromSegments(selection.topology); region != "" {
			csiVolume.VolumeContext[topologyKeyRegionStable] = region
		}
	}

	return &csi.CreateVolumeResponse{Volume: csiVolume}, nil
}

func (cs *controllerServer) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}

	// Invalid format means the volume was never created by this driver - treat as already deleted.
	if _, err := parseVolumeID(volumeID); err != nil {
		klog.Warningf("invalid volume ID format, treating as already deleted: %s", volumeID)
		return &csi.DeleteVolumeResponse{}, nil
	}

	unlock := cs.volumeLocks.Lock(volumeID)
	defer unlock()
	// no harm if volume already unpublished
	err := cs.unpublishVolume(ctx, volumeID)
	switch {
	case errors.Is(err, util.ErrVolumeUnpublished):
		klog.Warningf("volume not published: %s", volumeID)
	case err != nil:
		klog.Errorf("failed to unpublish volume, volumeID: %s err: %v", volumeID, err)
		return nil, classifyDeleteVolumeError(err)
	}

	// no harm if volume already deleted
	err = cs.deleteVolume(ctx, volumeID)
	if errors.Is(err, util.ErrVolumeNotFound) {
		// deleted in previous request?
		klog.Warningf("volume not exists: %s", volumeID)
	} else if err != nil {
		klog.Errorf("failed to delete volume, volumeID: %s err: %v", volumeID, err)
		return nil, classifyDeleteVolumeError(err)
	}

	return &csi.DeleteVolumeResponse{}, nil
}

func (cs *controllerServer) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	if len(req.GetVolumeCapabilities()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "volume capabilities are required")
	}

	spdkVol, err := parseVolumeID(volumeID)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "volume %q not found: %v", volumeID, err)
	}
	sbclient, err := util.NewsimplyBlockClient(ctx, spdkVol.clusterID, spdkVol.poolID)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if _, err := sbclient.VolumeInfo(ctx, spdkVol.lvolID, ""); err != nil {
		return nil, classifyValidateVolumeCapabilitiesError(err)
	}

	// make sure we support all requested caps
	for _, cap := range req.GetVolumeCapabilities() {
		supported := false
		for _, accessMode := range cs.Driver.GetVolumeCapabilityAccessModes() {
			if cap.GetAccessMode().GetMode() == accessMode.GetMode() {
				supported = true
				break
			}
		}
		if !supported {
			return &csi.ValidateVolumeCapabilitiesResponse{Message: ""}, nil
		}
	}
	return &csi.ValidateVolumeCapabilitiesResponse{
		Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
			VolumeCapabilities: req.GetVolumeCapabilities(),
		},
	}, nil
}

// reconcileExistingSnapshot handles a 409 from CreateSnapshot: the control plane
// says a snapshot with this name already exists. It lists snapshots and, if the
// existing one has the same source volume, returns it as success (CSI
// idempotency — this is our own snapshot from an earlier attempt). If the source
// differs, it is a real name conflict → AlreadyExists.
func reconcileExistingSnapshot(ctx context.Context, sbclient util.ClusterAPI, sourceLvolID, snapshotName string) (*csi.CreateSnapshotResponse, error) {
	snaps, err := sbclient.ListSnapshots(ctx)
	if err != nil {
		return nil, classifyCreateSnapshotError(err)
	}
	for _, s := range snaps {
		if s.Name != snapshotName {
			continue
		}
		if lvolIDFromURL(s.LvolURL) != sourceLvolID {
			return nil, status.Errorf(codes.AlreadyExists, "snapshot %q already exists with a different source volume", snapshotName)
		}
		creationTime := timestamppb.Now()
		if ts, perr := time.Parse(time.RFC3339Nano, s.CreatedAt); perr == nil {
			creationTime = timestamppb.New(ts)
		}
		return &csi.CreateSnapshotResponse{
			Snapshot: &csi.Snapshot{
				SizeBytes:      s.Size,
				SnapshotId:     fmt.Sprintf("%s:%s:%s", sbclient.ClusterID(), sbclient.PoolID(), s.UUID),
				SourceVolumeId: sourceLvolID,
				CreationTime:   creationTime,
				ReadyToUse:     true,
			},
		}, nil
	}
	return nil, status.Errorf(codes.Internal, "snapshot %q reported as existing but was not found", snapshotName)
}

func (cs *controllerServer) CreateSnapshot(ctx context.Context, req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	volumeID := req.GetSourceVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "source volume ID is required")
	}
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "snapshot name is required")
	}

	klog.Infof("CreateSnapshot : volumeID=%s", volumeID)
	unlock := cs.volumeLocks.Lock(volumeID)
	defer unlock()

	snapshotName := req.GetName()
	klog.Infof("CreateSnapshot : snapshotName=%s", snapshotName)
	spdkVol, err := parseVolumeID(volumeID)
	if err != nil {
		klog.Errorf("failed to get spdk volume, volumeID: %s err: %v", volumeID, err)
		return nil, status.Errorf(codes.InvalidArgument, "invalid source volume ID %q: %v", volumeID, err)
	}
	sbclient, err := util.NewsimplyBlockClient(ctx, spdkVol.clusterID, spdkVol.poolID)
	if err != nil {
		klog.Errorf("failed to create spdk client: %v", err)
		return nil, status.Error(codes.Unavailable, err.Error())
	}

	volSize, err := sbclient.GetVolumeSize(ctx, spdkVol.lvolID)
	klog.Infof("CreateSnapshot : volSize=%s", volSize)
	if err != nil {
		klog.Errorf("failed to get volume info, volumeID: %s err: %v", volumeID, err)
		return nil, classifyCreateSnapshotError(err)
	}
	size, err := strconv.ParseInt(volSize, 10, 64)
	if err != nil {
		klog.Errorf("failed to parse volume size, size: %s err: %v", volSize, err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	snapshotID, err := sbclient.CreateSnapshot(ctx, spdkVol.lvolID, snapshotName)
	klog.Infof("CreateSnapshot : snapshotID=%s", snapshotID)
	if err != nil {
		d := classifyCreateSnapshotError(err)
		if d.IsIdempotent() {
			// 409: the snapshot already exists. Reconcile — if it is ours (same
			// source) return it as success (CSI idempotency); if it belongs to a
			// different source, it is a genuine name conflict.
			return reconcileExistingSnapshot(ctx, sbclient, spdkVol.lvolID, snapshotName)
		}
		klog.Errorf("failed to create snapshot, volumeID: %s snapshotName: %s err: %v", volumeID, snapshotName, err)
		return nil, d
	}

	creationTime := timestamppb.Now()
	snapshotData := csi.Snapshot{
		SizeBytes:      size,
		SnapshotId:     snapshotID,
		SourceVolumeId: spdkVol.lvolID,
		CreationTime:   creationTime,
		ReadyToUse:     true,
	}

	return &csi.CreateSnapshotResponse{
		Snapshot: &snapshotData,
	}, nil
}

func (cs *controllerServer) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	csiSnapshotID := req.GetSnapshotId()
	if csiSnapshotID == "" {
		return nil, status.Error(codes.InvalidArgument, "snapshot ID is required")
	}

	unlock := cs.volumeLocks.Lock(csiSnapshotID)
	defer unlock()

	sbSnapshot, err := parseSnapshotID(csiSnapshotID)
	if err != nil {
		// Invalid format means the snapshot was never created by this driver — treat as already deleted.
		klog.Warningf("invalid snapshot ID format, treating as already deleted: %s", csiSnapshotID)
		return &csi.DeleteSnapshotResponse{}, nil
	}
	sbclient, err := util.NewsimplyBlockClient(ctx, sbSnapshot.clusterID, sbSnapshot.poolID)
	if err != nil {
		klog.Errorf("failed to create spdk client: %v", err)
		return nil, status.Error(codes.Unavailable, err.Error())
	}

	klog.Infof("Deleting Snapshot : csiSnapshotID=%s sbSnapshotID=%s", csiSnapshotID, sbSnapshot.snapshotID)

	err = sbclient.DeleteSnapshot(ctx, sbSnapshot.snapshotID)
	if err != nil {
		if d := classifyDeleteSnapshotError(err); !d.IsSuccess() {
			klog.Errorf("failed to delete snapshot, snapshotID: %s err: %v", csiSnapshotID, err)
			return nil, d
		}
		// already gone — idempotent success
		klog.Warningf("snapshot not found, treating as already deleted: %s", csiSnapshotID)
	}

	return &csi.DeleteSnapshotResponse{}, nil
}

func getIntParameter(params map[string]string, key string, defaultValue int) (int, error) {
	if valueStr, exists := params[key]; exists {
		value, err := strconv.Atoi(valueStr)
		if err != nil {
			return 0, fmt.Errorf("error converting %s: %w", key, err)
		}
		return value, nil
	}
	return defaultValue, nil
}

func getBoolParameter(params map[string]string, key string) bool {
	valueStr, exists := params[key]
	return exists && (valueStr == "true" || valueStr == "True")
}

func prepareCreateVolumeReq(ctx context.Context, req *csi.CreateVolumeRequest, capacityBytes int64) (*util.CreateLVolData, error) {
	params := req.GetParameters()

	priorClass, err := getIntParameter(params, "lvol_priority_class", 0)
	if err != nil {
		return nil, err
	}

	maxNamespace, err := getIntParameter(params, "max_namespace_per_subsys", 1)
	if err != nil {
		return nil, err
	}

	compression := getBoolParameter(params, "compression")
	encryption := getBoolParameter(params, "encryption")
	replicate := getBoolParameter(params, "replicate")

	pvcName, pvcNameSelected := params[CSIStorageNameKey]
	pvcNamespace, pvcNamespaceSelected := params[CSIStorageNamespaceKey]

	pvcFullName := pvcName
	if pvcNameSelected && pvcNamespaceSelected {
		pvcFullName = fmt.Sprintf("%s/%s", pvcNamespace, pvcName)
	}

	var pvcAnns map[string]string
	if pvcNameSelected && pvcNamespaceSelected {
		pvcAnns, err = fetchPVCAnnotations(ctx, pvcName, pvcNamespace)
		if err != nil {
			return nil, err
		}
	}

	hostID := pvcAnnotation(pvcAnns, annotationHostID, deprecatedAnnotationHostID)
	lvolID := pvcAnnotation(pvcAnns, annotationLvolID, deprecatedAnnotationLvolID)

	// QoS from StorageClass, overridable per-PVC via annotations.
	maxRWIOPS := params["qos_rw_iops"]
	maxRWmBytes := params["qos_rw_mbytes"]
	maxRmBytes := params["qos_r_mbytes"]
	maxWmBytes := params["qos_w_mbytes"]
	if pvcNameSelected && pvcNamespaceSelected {
		if v := pvcAnnotation(pvcAnns, annotationQoSRWIOPS, deprecatedAnnotationQoSRWIOPS); v != "" {
			maxRWIOPS = v
		}
		if v := pvcAnnotation(pvcAnns, annotationQoSRWMBps, deprecatedAnnotationQoSRWMBps); v != "" {
			maxRWmBytes = v
		}
		if v := pvcAnnotation(pvcAnns, annotationQoSRMBps, deprecatedAnnotationQoSRMBps); v != "" {
			maxRmBytes = v
		}
		if v := pvcAnnotation(pvcAnns, annotationQoSWMBps, deprecatedAnnotationQoSWMBps); v != "" {
			maxWmBytes = v
		}
	}

	createVolReq := util.CreateLVolData{
		LvolName:     req.GetName(),
		Size:         strconv.FormatInt(capacityBytes, 10),
		LvsName:      params["pool_name"],
		Fabric:       params["fabric"],
		MaxRWIOPS:    maxRWIOPS,
		MaxRWmBytes:  maxRWmBytes,
		MaxRmBytes:   maxRmBytes,
		MaxWmBytes:   maxWmBytes,
		MaxSize:      params["max_size"],
		MaxNamespace: maxNamespace,
		PriorClass:   priorClass,
		Compression:  compression,
		Encryption:   encryption,
		Replicate:    replicate,
		HostID:       hostID,
		LvolID:       lvolID,
		Namespaced:   maxNamespace > 1,
		PvcName:      pvcFullName,
	}
	return &createVolReq, nil
}

// reconcileExistingVolume handles a 409 (name already exists) on a volume create
// or clone. If an online volume with the name exists it is reused (after a size
// check); a non-online leftover from a failed earlier attempt is deleted so the
// caller can recreate. It returns the existing volume's UUID to reuse, "" to
// recreate, or an error (a size conflict, or a list/delete failure).
func reconcileExistingVolume(ctx context.Context, sbclient util.ClusterAPI, name string, requiredBytes int64) (string, error) {
	volumes, err := sbclient.ListVolumes(ctx)
	if err != nil {
		return "", err
	}
	for _, v := range volumes {
		if v.Name != name {
			continue
		}
		if strings.EqualFold(v.Status, "online") {
			if requiredBytes > 0 {
				aligned := util.AlignToGiBBytes(requiredBytes)
				if v.LvolSize != aligned {
					return "", status.Errorf(codes.AlreadyExists, "volume %q exists with size %d but requested %d", name, v.LvolSize, aligned)
				}
			}
			klog.Infof("reconcile: reusing online existing volume %q id=%s", name, v.UUID)
			return v.UUID, nil
		}
		// Non-online leftover from a failed attempt: it will never come online.
		klog.Warningf("reconcile: deleting non-online leftover volume %q id=%s status=%s", name, v.UUID, v.Status)
		if delErr := sbclient.DeleteVolume(ctx, v.UUID); delErr != nil {
			return "", delErr
		}
	}
	return "", nil
}

func (cs *controllerServer) createVolume(ctx context.Context, req *csi.CreateVolumeRequest, sbclient util.ClusterAPI) (*csi.Volume, error) {
	size := req.GetCapacityRange().GetRequiredBytes()
	if size == 0 {
		klog.Warningln("invalid volume size, resize to 1G")
		size = 1024 * 1024 * 1024
	}

	capacityBytes := util.AlignToGiBBytes(size)
	vol := csi.Volume{
		CapacityBytes: capacityBytes,
		VolumeContext: req.GetParameters(),
		ContentSource: req.GetVolumeContentSource(),
	}

	klog.V(5).Info("provisioning volume from SDK node..")
	poolName := req.GetParameters()["pool_name"]
	if req.GetVolumeContentSource() != nil {
		clonedVolume, clonedErr := cs.handleVolumeContentSource(ctx, req, poolName, &vol, capacityBytes)
		if clonedErr != nil {
			return nil, clonedErr
		}
		if clonedVolume != nil {
			return clonedVolume, nil
		}
	}

	createVolReq, err := prepareCreateVolumeReq(ctx, req, capacityBytes)
	if err != nil {
		return nil, err
	}

	// Store the effective QoS values into VolumeContext so the PV spec records
	// what was actually applied.
	vol.VolumeContext["qos_rw_iops"] = createVolReq.MaxRWIOPS
	vol.VolumeContext["qos_rw_mbytes"] = createVolReq.MaxRWmBytes
	vol.VolumeContext["qos_r_mbytes"] = createVolReq.MaxRmBytes
	vol.VolumeContext["qos_w_mbytes"] = createVolReq.MaxWmBytes

	volumeID, err := sbclient.CreateVolume(ctx, createVolReq)
	if err != nil {
		if errors.Is(err, util.ErrVolumeExists) {
			klog.Infof("createVolume: volume %q already exists, reconciling", req.GetName())
			existingUUID, rerr := reconcileExistingVolume(ctx, sbclient, req.GetName(), req.GetCapacityRange().GetRequiredBytes())
			if rerr != nil {
				return nil, rerr
			}
			if existingUUID != "" {
				vol.VolumeId = fmt.Sprintf("%s:%s:%s", sbclient.ClusterID(), sbclient.PoolID(), existingUUID)
				return &vol, nil
			}
			// The non-online leftover (if any) has been cleaned up; create fresh.
			volumeID, err = sbclient.CreateVolume(ctx, createVolReq)
			if err != nil {
				klog.Errorf("createVolume: recreate after cleanup failed: %v", err)
				return nil, err
			}
			vol.VolumeId = fmt.Sprintf("%s:%s:%s", sbclient.ClusterID(), sbclient.PoolID(), volumeID)
			return &vol, nil
		}
		klog.Errorf("error creating simplyBlock volume: %v", err)
		return nil, err
	}
	vol.VolumeId = fmt.Sprintf("%s:%s:%s", sbclient.ClusterID(), sbclient.PoolID(), volumeID)
	klog.V(5).Info("successfully created volume from Simplyblock with Volume ID: ", vol.GetVolumeId())

	return &vol, nil
}

func parseVolumeID(csiVolumeID string) (*spdkVolume, error) {
	// csiVolumeID format: {clusterUUID}:{poolUUID}:{lvolUUID}
	// e.g. 8ffac363-0c46-4714-a71b-f9c0b58a1269:df34f16c-...:8e2dcb9d-...
	vh, ok := volumehandle.Parse(csiVolumeID)
	if !ok {
		return nil, fmt.Errorf("invalid volume handle %q (expected {clusterID}:{poolID}:{lvolID})", csiVolumeID)
	}
	return &spdkVolume{
		clusterID: vh.ClusterID,
		poolID:    vh.PoolID,
		lvolID:    vh.VolumeID,
	}, nil
}

func parseSnapshotID(csiSnapshotID string) (*spdkSnapshot, error) {
	ids := strings.Split(csiSnapshotID, ":")
	switch len(ids) {
	case 3:
		// New 3-part format: {clusterID}:{poolID}:{snapshotID}
		return &spdkSnapshot{clusterID: ids[0], poolID: ids[1], snapshotID: ids[2]}, nil
	case 2:
		// Legacy 2-part format: {clusterID}:{snapshotID} — pool resolved at delete time
		return &spdkSnapshot{clusterID: ids[0], snapshotID: ids[1]}, nil
	default:
		return nil, fmt.Errorf("invalid snapshot ID format: %s", csiSnapshotID)
	}
}

func (cs *controllerServer) publishVolume(ctx context.Context, volumeID string, sbclient util.ClusterAPI) (map[string]string, error) {
	spdkVol, err := parseVolumeID(volumeID)
	if err != nil {
		return nil, err
	}
	err = sbclient.PublishVolume(ctx, spdkVol.lvolID)
	if err != nil {
		return nil, err
	}

	// hostNQN is not available in the controller path; pass empty string.
	// If the volume has allowed_hosts configured, this call will fail and the
	// node will re-fetch connection info at NodeStageVolume time using its own NQN.
	volumeInfo, err := sbclient.VolumeInfo(ctx, spdkVol.lvolID, "")
	if err != nil {
		klog.Warningf("failed to get volume info for %s (will be fetched at stage time): %v", spdkVol.lvolID, err)
		return map[string]string{}, nil
	}
	return volumeInfo, nil
}

func (cs *controllerServer) deleteVolume(ctx context.Context, volumeID string) error {
	spdkVol, err := parseVolumeID(volumeID)
	if err != nil {
		return err
	}
	sbclient, err := util.NewsimplyBlockClient(ctx, spdkVol.clusterID, spdkVol.poolID)
	if err != nil {
		return err
	}
	return sbclient.DeleteVolume(ctx, spdkVol.lvolID)
}

func (cs *controllerServer) unpublishVolume(ctx context.Context, volumeID string) error {
	spdkVol, err := parseVolumeID(volumeID)
	if err != nil {
		return err
	}
	sbclient, err := util.NewsimplyBlockClient(ctx, spdkVol.clusterID, spdkVol.poolID)
	if err != nil {
		return err
	}
	return sbclient.UnpublishVolume(ctx, spdkVol.lvolID)
}

func (cs *controllerServer) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	if req.GetCapacityRange() == nil {
		return nil, status.Error(codes.InvalidArgument, "capacity range is required")
	}

	unlock := cs.volumeLocks.Lock(volumeID)
	defer unlock()

	updatedSize := req.GetCapacityRange().GetRequiredBytes()

	// Simplyblock backends are GiB aligned, so we round up to GiB.
	capacityBytes := util.AlignToGiBBytes(updatedSize)

	spdkVol, err := parseVolumeID(volumeID)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid volume ID %q: %v", volumeID, err)
	}

	sbclient, err := util.NewsimplyBlockClient(ctx, spdkVol.clusterID, spdkVol.poolID)
	if err != nil {
		return nil, err
	}

	err = sbclient.ResizeVolume(ctx, spdkVol.lvolID, capacityBytes)
	if err != nil {
		klog.Errorf("failed to resize lvol, LVolID: %s err: %v", spdkVol.lvolID, err)
		return nil, classifyControllerExpandVolumeError(err)
	}
	return &csi.ControllerExpandVolumeResponse{
		CapacityBytes:         capacityBytes,
		NodeExpansionRequired: true,
	}, nil
}

// ListSnapshots lists all snapshots across all clusters
func (cs *controllerServer) ListSnapshots(ctx context.Context, req *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {

	var entries []*util.SnapshotResp
	clusters, err := ListClusters()
	if err != nil {
		return nil, err
	}

	for _, clusterID := range clusters {
		sbclient, err := util.NewsimplyBlockClient(ctx, clusterID, "")
		if err != nil {
			klog.Errorf("failed to create spdk client: %v", err)
			return nil, status.Error(codes.Unavailable, err.Error())
		}

		snapshotEntries, err := sbclient.ListSnapshots(ctx)
		if err != nil {
			return nil, classifyListSnapshotsError(err)
		}
		entries = append(entries, snapshotEntries...)
	}

	var all []*csi.ListSnapshotsResponse_Entry
	for _, entry := range entries {
		snapshotID := fmt.Sprintf("%s:%s:%s", entry.ClusterID, entry.PoolID, entry.UUID)
		sourceVolumeID := lvolIDFromURL(entry.LvolURL)

		if req.GetSnapshotId() != "" && req.GetSnapshotId() != snapshotID {
			continue
		}
		if req.GetSourceVolumeId() != "" && req.GetSourceVolumeId() != sourceVolumeID {
			continue
		}

		createdAt, _ := time.Parse(time.RFC3339Nano, entry.CreatedAt)
		all = append(all, &csi.ListSnapshotsResponse_Entry{
			Snapshot: &csi.Snapshot{
				SizeBytes:      entry.Size,
				SnapshotId:     snapshotID,
				SourceVolumeId: sourceVolumeID,
				CreationTime:   timestamppb.New(createdAt),
				ReadyToUse:     true,
			},
		})
	}

	page, nextToken, err := paginateSnapshots(all, req.GetStartingToken(), int(req.GetMaxEntries()))
	if err != nil {
		return nil, err
	}

	return &csi.ListSnapshotsResponse{
		Entries:   page,
		NextToken: nextToken,
	}, nil
}

// paginateSnapshots returns one page of entries starting at startingToken (an
// absolute index from a prior call). pageSize 0 returns all remaining entries.
func paginateSnapshots(all []*csi.ListSnapshotsResponse_Entry, startingToken string, pageSize int) ([]*csi.ListSnapshotsResponse_Entry, string, error) {
	start := 0
	if startingToken != "" {
		var parseErr error
		start, parseErr = strconv.Atoi(startingToken)
		if parseErr != nil || start < 0 {
			return nil, "", status.Errorf(codes.Aborted, "invalid starting token: %q", startingToken)
		}
	}
	if start > len(all) {
		start = len(all)
	}
	page := all[start:]

	var nextToken string
	if pageSize > 0 && len(page) > pageSize {
		nextToken = strconv.Itoa(start + pageSize)
		page = page[:pageSize]
	}

	return page, nextToken, nil
}

// lvolIDFromURL extracts the volume UUID from a URL path like
// /api/v2/clusters/{id}/storage-pools/{id}/volumes/{volume_id}
func lvolIDFromURL(lvolURL string) string {
	u := strings.TrimRight(lvolURL, "/")
	if idx := strings.LastIndex(u, "/"); idx >= 0 {
		return u[idx+1:]
	}
	return lvolURL
}

func ListClusters() (clusterIds []string, err error) {
	var clusters util.ClustersInfo
	secretFile := util.FromEnv("SPDKCSI_SECRET", "/etc/spdkcsi-secret/secret.json")
	err = util.ParseJSONFile(secretFile, &clusters)
	if err != nil {
		klog.Errorf("failed to parse secret file: %v", err)
		return
	}
	for _, cluster := range clusters.Clusters {
		clusterIds = append(clusterIds, cluster.ClusterID)
	}
	return
}

// func (cs *controllerServer) ListVolumes(_ context.Context, _ *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
// 	volumes := []*csi.ListVolumesResponse_Entry{}

// 	volumeIDs, err := cs.spdkNode.ListVolumes()
// 	if err != nil {
// 		klog.Errorf("failed to list volumes: %v", err)
// 		return nil, status.Error(codes.Internal, err.Error())
// 	}

// 	for _, volumeID := range volumeIDs {
// 		volumeInfo, err := cs.spdkNode.VolumeInfo(volumeID.UUID)
// 		if err != nil {
// 			klog.Errorf("failed to get volume info for volume %s: %v", volumeID.UUID, err)
// 			return nil, status.Error(codes.NotFound, err.Error())
// 		}
// 		volume := &csi.Volume{
// 			VolumeId:      volumeID.UUID,
// 			VolumeContext: volumeInfo,
// 		}

// 		volumes = append(volumes, &csi.ListVolumesResponse_Entry{
// 			Volume: volume,
// 		})
// 	}

// 	return &csi.ListVolumesResponse{
// 		Entries: volumes,
// 	}, nil
// }

//	func (cs *controllerServer) GetCapacity(ctx context.Context, req *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
//		return nil, status.Error(codes.Unimplemented, "")
//	}

func (cs *controllerServer) ControllerGetVolume(ctx context.Context, req *csi.ControllerGetVolumeRequest) (*csi.ControllerGetVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	unlock := cs.volumeLocks.Lock(volumeID)
	defer unlock()

	spdkVol, err := parseVolumeID(volumeID)
	if err != nil {
		return nil, err
	}

	sbclient, err := util.NewsimplyBlockClient(ctx, spdkVol.clusterID, spdkVol.poolID)
	if err != nil {
		klog.Errorf("failed to create spdk client: %v", err)
		return nil, status.Error(codes.Unavailable, err.Error())
	}

	volumeInfo, err := sbclient.VolumeInfo(ctx, spdkVol.lvolID, "")
	if err != nil {
		klog.Errorf("failed to get spdkVol for %s: %v", volumeID, err)

		return &csi.ControllerGetVolumeResponse{
			Volume: &csi.Volume{
				VolumeId: volumeID,
			},
			Status: &csi.ControllerGetVolumeResponse_VolumeStatus{
				VolumeCondition: &csi.VolumeCondition{
					Abnormal: true,
					Message:  err.Error(),
				},
			},
		}, nil
	}

	volume := &csi.Volume{
		VolumeId:      spdkVol.lvolID,
		VolumeContext: volumeInfo,
	}

	return &csi.ControllerGetVolumeResponse{
		Volume: volume,
		Status: &csi.ControllerGetVolumeResponse_VolumeStatus{
			VolumeCondition: &csi.VolumeCondition{
				Abnormal: false,
				Message:  "",
			},
		},
	}, nil
}

func newControllerServer(d *csicommon.CSIDriver) (*controllerServer, error) {
	server := controllerServer{
		DefaultControllerServer: csicommon.NewDefaultControllerServer(d),
		volumeLocks:             util.NewVolumeLocks(),
	}
	return &server, nil
}

func (cs *controllerServer) handleVolumeContentSource(ctx context.Context, req *csi.CreateVolumeRequest, poolName string, vol *csi.Volume, sizeBytes int64) (*csi.Volume, error) {
	volumeSource := req.GetVolumeContentSource()
	switch volumeSource.GetType().(type) {
	case *csi.VolumeContentSource_Snapshot:
		return cs.handleSnapshotSource(ctx, volumeSource.GetSnapshot(), req, poolName, vol, sizeBytes)
	case *csi.VolumeContentSource_Volume:
		return cs.handleVolumeSource(ctx, volumeSource.GetVolume(), req, poolName, vol, sizeBytes)
	default:
		return nil, status.Errorf(codes.InvalidArgument, "%v not a proper volume source", volumeSource)
	}
}

func (cs *controllerServer) handleSnapshotSource(ctx context.Context, snapshot *csi.VolumeContentSource_SnapshotSource, req *csi.CreateVolumeRequest, poolName string, vol *csi.Volume, sizeBytes int64) (*csi.Volume, error) {
	if snapshot == nil {
		return nil, nil
	}
	csiSnapshotID := snapshot.GetSnapshotId()
	sbSnapshot, err := parseSnapshotID(csiSnapshotID)
	if err != nil {
		klog.Errorf("failed to get spdk snapshot, csiSnapshotID: %s err: %v", csiSnapshotID, err)
		return nil, status.Errorf(codes.NotFound, "snapshot %q not found: %v", csiSnapshotID, err)
	}
	// Use destination pool (from StorageClass params), not source snapshot pool.
	sbclient, err := util.NewsimplyBlockClient(ctx, sbSnapshot.clusterID, poolName)
	if err != nil {
		klog.Errorf("failed to create spdk client: %v", err)
		return nil, status.Error(codes.Unavailable, err.Error())
	}

	klog.Infof("CreateSnapshot : snapshotID=%s", sbSnapshot.snapshotID)
	snapshotName := req.GetName()
	params := req.GetParameters()
	pvcName, pvcNameSelected := params[CSIStorageNameKey]
	pvcNamespace, pvcNamespaceSelected := params[CSIStorageNamespaceKey]

	pvcFullName := pvcName
	if pvcNameSelected && pvcNamespaceSelected {
		pvcFullName = fmt.Sprintf("%s/%s", pvcNamespace, pvcName)
	}
	// Use raw bytes to avoid decimal/binary unit ambiguity in clone sizing.
	newSize := strconv.FormatInt(sizeBytes, 10)
	volumeID, err := sbclient.CloneSnapshot(ctx, sbSnapshot.snapshotID, snapshotName, newSize, pvcFullName)
	if err != nil {
		if !classifyCreateVolumeError(err).IsIdempotent() {
			klog.Errorf("error cloning snapshot: %v", err)
			return nil, err
		}
		// 409: a clone with this name already exists — reconcile it.
		existingUUID, rerr := reconcileExistingVolume(ctx, sbclient, snapshotName, sizeBytes)
		if rerr != nil {
			return nil, rerr
		}
		if existingUUID != "" {
			vol.VolumeId = fmt.Sprintf("%s:%s:%s", sbclient.ClusterID(), sbclient.PoolID(), existingUUID)
			return vol, nil
		}
		volumeID, err = sbclient.CloneSnapshot(ctx, sbSnapshot.snapshotID, snapshotName, newSize, pvcFullName)
		if err != nil {
			klog.Errorf("error re-cloning snapshot after cleanup: %v", err)
			return nil, err
		}
	}
	vol.VolumeId = fmt.Sprintf("%s:%s:%s", sbclient.ClusterID(), sbclient.PoolID(), volumeID)
	klog.V(5).Info("successfully Restored Snapshot from Simplyblock with Volume ID: ", vol.GetVolumeId())

	return vol, nil
}

func (cs *controllerServer) handleVolumeSource(ctx context.Context, srcVolume *csi.VolumeContentSource_VolumeSource, req *csi.CreateVolumeRequest, poolName string, vol *csi.Volume, sizeBytes int64) (*csi.Volume, error) {
	if srcVolume == nil {
		return nil, nil
	}
	srcVolumeID := srcVolume.GetVolumeId()

	klog.Infof("srcVolumeID=%s", srcVolumeID)

	cloneName := req.GetName()
	params := req.GetParameters()
	pvcName, pvcNameSelected := params[CSIStorageNameKey]
	pvcNamespace, pvcNamespaceSelected := params[CSIStorageNamespaceKey]
	pvcFullName := pvcName
	if pvcNameSelected && pvcNamespaceSelected {
		pvcFullName = fmt.Sprintf("%s/%s", pvcNamespace, pvcName)
	}

	spdkVol, err := parseVolumeID(srcVolumeID)
	if err != nil {
		klog.Errorf("failed to get spdk volume, srcVolumeID: %s err: %v", srcVolumeID, err)
		return nil, status.Errorf(codes.NotFound, "source volume %q not found: %v", srcVolumeID, err)
	}
	// Volume clone goes to the same pool as the source volume.
	sbclient, err := util.NewsimplyBlockClient(ctx, spdkVol.clusterID, spdkVol.poolID)

	if err != nil {
		klog.Errorf("failed to create spdk client: %v", err)
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	// Use raw bytes to avoid decimal/binary unit ambiguity in clone sizing.
	newSize := strconv.FormatInt(sizeBytes, 10)
	klog.Infof("CloneVolume : cloneName=%s", cloneName)
	volumeID, err := sbclient.CloneVolume(ctx, spdkVol.lvolID, cloneName, newSize, pvcFullName)
	if err != nil {
		if !classifyCreateVolumeError(err).IsIdempotent() {
			klog.Errorf("error cloning volume: %v", err)
			return nil, err
		}
		// 409: a clone with this name already exists — reconcile it.
		existingUUID, rerr := reconcileExistingVolume(ctx, sbclient, cloneName, sizeBytes)
		if rerr != nil {
			return nil, rerr
		}
		if existingUUID != "" {
			vol.VolumeId = fmt.Sprintf("%s:%s:%s", sbclient.ClusterID(), sbclient.PoolID(), existingUUID)
			return vol, nil
		}
		volumeID, err = sbclient.CloneVolume(ctx, spdkVol.lvolID, cloneName, newSize, pvcFullName)
		if err != nil {
			klog.Errorf("error re-cloning volume after cleanup: %v", err)
			return nil, err
		}
	}
	vol.VolumeId = fmt.Sprintf("%s:%s:%s", sbclient.ClusterID(), sbclient.PoolID(), volumeID)
	klog.V(5).Info("successfully created clone volume from Simplyblock with Volume ID: ", vol.GetVolumeId())

	return vol, nil
}

func fetchPVCAnnotations(ctx context.Context, pvcName, pvcNamespace string) (map[string]string, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		klog.Errorf("failed to get in-cluster config: %v", err)
		return nil, fmt.Errorf("could not get in-cluster config: %w", err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		klog.Errorf("failed to create clientset: %v", err)
		return nil, fmt.Errorf("could not create clientset: %w", err)
	}

	pvc, err := clientset.CoreV1().PersistentVolumeClaims(pvcNamespace).Get(ctx, pvcName, metav1.GetOptions{})
	if err != nil {
		klog.Errorf("failed to get PVC %s in namespace %s: %v", pvcName, pvcNamespace, err)
		return nil, fmt.Errorf("could not get PVC %s in namespace %s: %w", pvcName, pvcNamespace, err)
	}

	return pvc.ObjectMeta.Annotations, nil
}

// pvcAnnotation returns the value for newKey, falling back to deprecatedKey for backward compat.
func pvcAnnotation(annotations map[string]string, newKey, deprecatedKey string) string {
	if v, ok := annotations[newKey]; ok {
		return v
	}
	return annotations[deprecatedKey]
}
