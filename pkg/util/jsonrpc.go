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

package util

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"k8s.io/klog"
)

// SpdkNode defines interface for SPDK storage node
//
//   - Info returns node info(rpc url) for debugging purpose
//   - LvStores returns available volume stores(name, size, etc) on that node.
//   - VolumeInfo returns a string map to be passed to client node. Client node
//     needs these info to mount the target. E.g, target IP, service port, nqn.
//   - Create/Delete/Publish/UnpublishVolume per CSI controller service spec.
//
// NOTE: concurrency, idempotency, message ordering
//
// In below text, "implementation" refers to the code implements this interface,
// and "caller" is the code uses the implementation.
//
// Concurrency requirements for implementation and caller:
//   - Implementation should make sure CreateVolume is thread
//     safe. Caller is free to request creating multiple volumes in
//     same volume store concurrently, no data race should happen.
//   - Implementation should make sure
//     PublishVolume/UnpublishVolume/DeleteVolume for *different
//     volumes* thread safe. Caller may issue these requests to
//     *different volumes", in same volume store or not, concurrently.
//   - PublishVolume/UnpublishVolume/DeleteVolume for *same volume* is
//     not thread safe, concurrent access may lead to data
//     race. Caller must serialize these calls to *same volume*,
//     possibly by mutex or message queue per volume.
//   - Implementation should make sure LvStores and VolumeInfo are
//     thread safe, but it doesn't lock the returned resources. It
//     means caller should adopt optimistic concurrency control and
//     retry on specific failures.  E.g, caller calls LvStores and
//     finds a volume store with enough free space, it calls
//     CreateVolume but fails with "not enough space" because another
//     caller may issue similar request at same time. The failed
//     caller may redo above steps(call LvStores, pick volume store,
//     CreateVolume) under this condition, or it can simply fail.
//
// Idempotent requirements for implementation:
// Per CSI spec, it's possible that same request been sent multiple times due to
// issues such as a temporary network failure. Implementation should have basic
// logic to deal with idempotency.
// E.g, ignore publishing an already published volume.
//
// Out of order messages handling for implementation:
// Out of order message may happen in kubernetes CSI framework. E.g, unpublish
// an already deleted volume. The baseline is there should be no code crash or
// data corruption under these conditions. Implementation may try to detect and
// report errors if possible.

// errors deserve special care
var (
	ErrJSONNoSpaceLeft  = errors.New("json: No space left")
	ErrJSONNoSuchDevice = errors.New("json: No such device")

	// internal errors
	ErrVolumeDeleted     = errors.New("volume deleted")
	ErrVolumeUnpublished = errors.New("volume not published")
)

type SpdkNode interface {
	Info() string
	LvStores() ([]LvStore, error)
	VolumeInfo(lvolID string, hostNQN string) (map[string]string, error)
	CreateVolume(lvolName, lvsName string, sizeMiB int64) (string, error)
	GetVolume(lvolName, lvsName string) (string, error)
	DeleteVolume(lvolID string) error
	PublishVolume(lvolID string) error
	UnpublishVolume(lvolID string) error
	CreateSnapshot(lvolName, snapshotName string) (string, error)
	DeleteSnapshot(snapshotID string) error
}

// logical volume store
type LvStore struct {
	Name         string
	UUID         string
	TotalSizeMiB int64
	FreeSizeMiB  int64
}

type LvolConnectResp struct {
	Nqn            string `json:"nqn"`
	ReconnectDelay int    `json:"reconnect-delay"`
	NrIoQueues     int    `json:"nr-io-queues"`
	CtrlLossTmo    int    `json:"ctrl-loss-tmo"`
	Port           int    `json:"port"`
	TargetType     string `json:"transport"`
	IP             string `json:"ip"`
	Connect        string `json:"connect"`
	NSID           int    `json:"ns_id"`
	HostIface      string `json:"host-iface,omitempty"`
}

type connectionInfo struct {
	IP   string `json:"ip"`
	Port int    `json:"port"`
}

// LvolResp is the v2 VolumeDTO returned by the SimplyBlock API
type LvolResp struct {
	Name     string `json:"name"`
	UUID     string `json:"id"`
	LvolSize int64  `json:"size"`
	Status   string `json:"status"`
}

// RPCClient holds the connection information to the SimplyBlock Cluster
type RPCClient struct {
	ClusterID     string
	PoolID        string
	ClusterIP     string
	ClusterSecret string
	HTTPClient    *http.Client
}

// ClusterStatus is a partial view of the GET /clusters/{id}/ response in v2.
type ClusterStatus struct {
	Status string `json:"status"`
}

// CSIPoolsResp is the response of GET /storage-pools/ — field tags match v2 StoragePoolDTO
type CSIPoolsResp struct {
	Name string `json:"name"`
	UUID string `json:"id"`
}

// SnapshotResp is the response of GET /snapshots/ — field tags match v2 SnapshotDTO
type SnapshotResp struct {
	Name      string `json:"name"`
	UUID      string `json:"id"`
	Size      int64  `json:"size"`
	LvolURL   string `json:"lvol"` // URL path to source volume (may be empty in list responses)
	PoolID    string `json:"-"`    // populated after fetch, not from JSON
	ClusterID string `json:"-"`    // populated after fetch, not from JSON
}

// CreateVolResp is the response for volume create (legacy, kept for compat)
type CreateVolResp struct {
	LVols []string `json:"lvols"`
}

// ResizeVolReq is the request body for v2 volume resize
type ResizeVolReq struct {
	Size int64 `json:"size"`
}

// Error represents SBCLI's common error response
type Error struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// MasterLvol is the response of /storage-pools/{pool_uuid}/master-lvols
type MasterLvol struct {
	ID            string `json:"Id"`
	Name          string `json:"Name"`
	Size          string `json:"Size"`
	Hostname      string `json:"Hostname"`
	Status        string `json:"Status"`
	Namespaces    int    `json:"Namespaces"`
	MaxNamespaces int    `json:"MaxNamespaces"`
}

func (client *RPCClient) info() string {
	return client.ClusterID
}

// --- v2 URL path helpers ---

func (client *RPCClient) v2pools() string {
	return fmt.Sprintf("api/v2/clusters/%s/storage-pools", client.ClusterID)
}

func (client *RPCClient) v2pool(poolID string) string {
	return fmt.Sprintf("api/v2/clusters/%s/storage-pools/%s", client.ClusterID, poolID)
}

func (client *RPCClient) v2volumes() string {
	return fmt.Sprintf("api/v2/clusters/%s/storage-pools/%s/volumes", client.ClusterID, client.PoolID)
}

func (client *RPCClient) v2volume(volumeID string) string {
	return fmt.Sprintf("api/v2/clusters/%s/storage-pools/%s/volumes/%s/", client.ClusterID, client.PoolID, volumeID)
}

func (client *RPCClient) v2snapshots() string {
	return fmt.Sprintf("api/v2/clusters/%s/storage-pools/%s/snapshots", client.ClusterID, client.PoolID)
}

func (client *RPCClient) v2snapshot(snapshotID string) string {
	return fmt.Sprintf("api/v2/clusters/%s/storage-pools/%s/snapshots/%s/", client.ClusterID, client.PoolID, snapshotID)
}

func (client *RPCClient) v2storageNode(nodeID string) string {
	return fmt.Sprintf("api/v2/clusters/%s/storage-nodes/%s/", client.ClusterID, nodeID)
}

// --- API methods ---

// lvStores returns all available storage pools
func (client *RPCClient) lvStores() ([]LvStore, error) {
	out, err := client.CallSBCLI("GET", client.v2pools()+"/", nil)
	if err != nil {
		return nil, err
	}

	b, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal pools response: %w", err)
	}
	var result []CSIPoolsResp
	if err := json.Unmarshal(b, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal pools response: %w", err)
	}

	lvs := make([]LvStore, len(result))
	for i := range result {
		lvs[i].Name = result[i].Name
		lvs[i].UUID = result[i].UUID
	}
	return lvs, nil
}

// createVolume creates a logical volume and returns its UUID
func (client *RPCClient) createVolume(params *CreateLVolData) (string, error) {
	out, err := client.CallSBCLI("POST", client.v2volumes()+"/", params)
	if err != nil {
		if errorMatches(err, ErrJSONNoSpaceLeft) {
			err = ErrJSONNoSpaceLeft
		}
		return "", err
	}
	lvolID, ok := out.(string)
	if !ok {
		return "", fmt.Errorf("unexpected response for createVolume: %T %v", out, out)
	}
	return lvolID, nil
}

// getVolumeByUUID fetches a single volume by its UUID
func (client *RPCClient) getVolumeByUUID(lvolID string) (*LvolResp, error) {
	out, err := client.CallSBCLI("GET", client.v2volume(lvolID), nil)
	if err != nil {
		if errorMatches(err, ErrJSONNoSuchDevice) {
			err = ErrJSONNoSuchDevice
		}
		return nil, err
	}
	b, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal volume response: %w", err)
	}
	var result LvolResp
	if err := json.Unmarshal(b, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal volume response: %w", err)
	}
	return &result, nil
}

// getVolumeByName lists all volumes in the pool and returns the one with matching name
func (client *RPCClient) getVolumeByName(name string) (*LvolResp, error) {
	volumes, err := client.listVolumes()
	if err != nil {
		return nil, err
	}
	for _, v := range volumes {
		if v.Name == name {
			return v, nil
		}
	}
	return nil, ErrJSONNoSuchDevice
}

// getVolume accepts either a UUID or a "poolName/volName" path (legacy callers)
func (client *RPCClient) getVolume(lvolIDOrPath string) (*LvolResp, error) {
	if strings.Contains(lvolIDOrPath, "/") {
		// "poolName/volName" — extract the volume name and search by name
		parts := strings.SplitN(lvolIDOrPath, "/", 2)
		return client.getVolumeByName(parts[1])
	}
	return client.getVolumeByUUID(lvolIDOrPath)
}

// listVolumes returns all volumes in the pool
func (client *RPCClient) listVolumes() ([]*LvolResp, error) {
	out, err := client.CallSBCLI("GET", client.v2volumes()+"/", nil)
	if err != nil {
		return nil, err
	}
	b, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal volumes response: %w", err)
	}
	var results []*LvolResp
	if err := json.Unmarshal(b, &results); err != nil {
		return nil, fmt.Errorf("failed to unmarshal volumes response: %w", err)
	}
	return results, nil
}

// getVolumeInfo returns the NVMe connection info for a volume
func (client *RPCClient) getVolumeInfo(lvolID string, hostNQN string) (map[string]string, error) {
	path := client.v2volume(lvolID) + "connect"
	if hostNQN != "" {
		path += "?host_nqn=" + url.QueryEscape(hostNQN)
	}

	out, err := client.CallSBCLI("GET", path, nil)
	if err != nil {
		if errorMatches(err, ErrJSONNoSuchDevice) {
			err = ErrJSONNoSuchDevice
		}
		return nil, err
	}

	byteData, err := json.Marshal(out)
	if err != nil {
		return nil, err
	}

	var result []*LvolConnectResp
	if err := json.Unmarshal(byteData, &result); err != nil {
		return nil, err
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("empty connect response for volume %s", lvolID)
	}

	var connections []connectionInfo
	for _, r := range result {
		connections = append(connections, connectionInfo{IP: r.IP, Port: r.Port})
	}

	_, model := getLvolIDFromNQN(result[0].Nqn)
	connectionsData, err := json.Marshal(connections)
	if err != nil {
		return nil, err
	}

	return map[string]string{
		"name":           lvolID,
		"uuid":           lvolID,
		"nqn":            result[0].Nqn,
		"reconnectDelay": strconv.Itoa(result[0].ReconnectDelay),
		"nrIoQueues":     strconv.Itoa(result[0].NrIoQueues),
		"ctrlLossTmo":    strconv.Itoa(result[0].CtrlLossTmo),
		"model":          model,
		"targetType":     result[0].TargetType,
		"connections":    string(connectionsData),
		"nsId":           strconv.Itoa(result[0].NSID),
		"hostIface":      result[0].HostIface,
	}, nil
}

// deleteVolume deletes a volume by UUID
func (client *RPCClient) deleteVolume(lvolID string) error {
	_, err := client.CallSBCLI("DELETE", client.v2volume(lvolID), nil)
	if errorMatches(err, ErrJSONNoSuchDevice) {
		err = ErrJSONNoSuchDevice
	}
	return err
}

// getPoolUUIDByName resolves a pool name to its UUID
func (client *RPCClient) getPoolUUIDByName(poolName string) (string, error) {
	pools, err := client.lvStores()
	if err != nil {
		return "", err
	}
	for _, p := range pools {
		if p.Name == poolName {
			return p.UUID, nil
		}
	}
	return "", fmt.Errorf("pool %q not found", poolName)
}

// getMasterLvols returns master lvols for a pool
func (client *RPCClient) getMasterLvols(poolUUID string) ([]MasterLvol, error) {
	path := client.v2pool(poolUUID) + "/master-lvols"
	out, err := client.CallSBCLI("GET", path, nil)
	if err != nil {
		return nil, err
	}
	if out == nil {
		return []MasterLvol{}, nil
	}
	b, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal master lvols response: %w", err)
	}
	var result []MasterLvol
	if err := json.Unmarshal(b, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal master lvols response: %w", err)
	}
	if result == nil {
		return []MasterLvol{}, nil
	}
	return result, nil
}

// resizeVolume resizes a volume
func (client *RPCClient) resizeVolume(lvolID string, size int64) (bool, error) {
	_, err := client.CallSBCLI("PUT", client.v2volume(lvolID), &ResizeVolReq{Size: size})
	if err != nil {
		return false, err
	}
	return true, nil
}

// cloneVolume clones a volume by UUID, returning the new volume's UUID
func (client *RPCClient) cloneVolume(lvolID, cloneName, newSize, pvcName string) (string, error) {
	path := client.v2volume(lvolID) + "clone?clone_name=" + url.QueryEscape(cloneName)
	if newSize != "" {
		path += "&new_size=" + url.QueryEscape(newSize)
	}
	if pvcName != "" {
		path += "&pvc_name=" + url.QueryEscape(pvcName)
	}

	klog.V(5).Infof("cloneVolume size: %s", newSize)

	out, err := client.CallSBCLI("POST", path, nil)
	if err != nil {
		if errorMatches(err, ErrJSONNoSpaceLeft) {
			err = ErrJSONNoSpaceLeft
		}
		return "", err
	}
	lvID, ok := out.(string)
	if !ok {
		return "", fmt.Errorf("unexpected response for cloneVolume: %T %v", out, out)
	}
	return lvID, nil
}

// cloneSnapshot creates a new volume from a snapshot, returning the new volume's UUID
func (client *RPCClient) cloneSnapshot(snapshotID, cloneName, newSize, pvcName string) (string, error) {
	params := struct {
		Name       string `json:"name"`
		SnapshotID string `json:"snapshot_id"`
		Size       string `json:"size,omitempty"`
		PVCName    string `json:"pvc_name,omitempty"`
	}{
		Name:       cloneName,
		SnapshotID: snapshotID,
		Size:       newSize,
		PVCName:    pvcName,
	}

	klog.V(5).Infof("cloneSnapshot size: %s", newSize)

	out, err := client.CallSBCLI("POST", client.v2volumes()+"/", &params)
	if err != nil {
		if errorMatches(err, ErrJSONNoSpaceLeft) {
			err = ErrJSONNoSpaceLeft
		}
		return "", err
	}
	lvolID, ok := out.(string)
	if !ok {
		return "", fmt.Errorf("unexpected response for cloneSnapshot: %T %v", out, out)
	}
	return lvolID, nil
}

// listSnapshots returns all snapshots in the pool
func (client *RPCClient) listSnapshots() ([]*SnapshotResp, error) {
	out, err := client.CallSBCLI("GET", client.v2snapshots()+"/", nil)
	if err != nil {
		return nil, err
	}
	b, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal snapshots response: %w", err)
	}
	var results []*SnapshotResp
	if err := json.Unmarshal(b, &results); err != nil {
		return nil, fmt.Errorf("failed to unmarshal snapshots response: %w", err)
	}
	return results, nil
}

// listAllSnapshots iterates every pool in the cluster and collects all snapshots.
// Used when PoolID is not set (e.g. ListSnapshots CSI RPC).
func (client *RPCClient) listAllSnapshots() ([]*SnapshotResp, error) {
	pools, err := client.lvStores()
	if err != nil {
		return nil, err
	}
	var all []*SnapshotResp
	savedPool := client.PoolID
	defer func() { client.PoolID = savedPool }()

	for _, pool := range pools {
		client.PoolID = pool.UUID
		snaps, err := client.listSnapshots()
		if err != nil {
			return nil, err
		}
		for _, s := range snaps {
			s.PoolID = pool.UUID
			s.ClusterID = client.ClusterID
		}
		all = append(all, snaps...)
	}
	return all, nil
}

// snapshot creates a snapshot of a volume and returns the snapshot UUID
func (client *RPCClient) snapshot(lvolID, snapShotName string) (string, error) {
	params := struct {
		Name string `json:"name"`
	}{Name: snapShotName}

	path := client.v2volume(lvolID) + "snapshots"
	out, err := client.CallSBCLI("POST", path, &params)
	if err != nil {
		if errorMatches(err, ErrJSONNoSpaceLeft) {
			err = ErrJSONNoSpaceLeft
		}
		return "", err
	}
	snapshotID, ok := out.(string)
	if !ok {
		return "", fmt.Errorf("unexpected response for snapshot: %T %v", out, out)
	}
	return snapshotID, nil
}

// deleteSnapshot deletes a snapshot.
// If PoolID is not set (legacy 2-part snapshot IDs), it scans all pools.
func (client *RPCClient) deleteSnapshot(snapshotID string) error {
	if client.PoolID == "" {
		return client.deleteSnapshotScanPools(snapshotID)
	}
	_, err := client.CallSBCLI("DELETE", client.v2snapshot(snapshotID), nil)
	if errorMatches(err, ErrJSONNoSuchDevice) {
		err = ErrJSONNoSuchDevice
	}
	return err
}

// deleteSnapshotScanPools finds a snapshot across all pools and deletes it.
// Used for backward compat with legacy 2-part CSI snapshot IDs that have no pool info.
func (client *RPCClient) deleteSnapshotScanPools(snapshotID string) error {
	pools, err := client.lvStores()
	if err != nil {
		return fmt.Errorf("failed to list pools while deleting snapshot %s: %w", snapshotID, err)
	}
	savedPool := client.PoolID
	defer func() { client.PoolID = savedPool }()

	for _, pool := range pools {
		client.PoolID = pool.UUID
		_, err := client.CallSBCLI("DELETE", client.v2snapshot(snapshotID), nil)
		if err == nil {
			return nil
		}
		if !strings.Contains(err.Error(), "404") && !strings.Contains(err.Error(), "Not Found") {
			return err
		}
	}
	return fmt.Errorf("snapshot %s not found in any pool", snapshotID)
}

// findPoolForVolume scans all pools to find the one containing the given volume UUID,
// then sets client.PoolID. No-op if PoolID is already set.
func (client *RPCClient) findPoolForVolume(lvolID string) error {
	if client.PoolID != "" {
		return nil
	}
	pools, err := client.lvStores()
	if err != nil {
		return fmt.Errorf("failed to list pools while resolving pool for volume %s: %w", lvolID, err)
	}
	for _, pool := range pools {
		client.PoolID = pool.UUID
		_, err := client.getVolumeByUUID(lvolID)
		if err == nil {
			return nil
		}
		if !errorMatches(err, ErrJSONNoSuchDevice) && !strings.Contains(err.Error(), "404") {
			client.PoolID = ""
			return fmt.Errorf("unexpected error searching for volume %s in pool %s: %w", lvolID, pool.UUID, err)
		}
	}
	client.PoolID = ""
	return fmt.Errorf("volume %s not found in any pool", lvolID)
}

// getLvolConnections returns the raw NVMe-oF connection list for a volume.
func (client *RPCClient) getLvolConnections(lvolID, hostNQN string) ([]*LvolConnectResp, error) {
	path := client.v2volume(lvolID) + "connect"
	if hostNQN != "" {
		path += "?host_nqn=" + url.QueryEscape(hostNQN)
	}
	out, err := client.CallSBCLI("GET", path, nil)
	if err != nil {
		return nil, err
	}
	b, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal connections response: %w", err)
	}
	var result []*LvolConnectResp
	if err := json.Unmarshal(b, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal connections response: %w", err)
	}
	return result, nil
}

// getStorageNodeStatus returns the status string for a storage node by UUID.
func (client *RPCClient) getStorageNodeStatus(nodeID string) (string, error) {
	out, err := client.CallSBCLI("GET", client.v2storageNode(nodeID), nil)
	if err != nil {
		return "", err
	}
	b, err := json.Marshal(out)
	if err != nil {
		return "", fmt.Errorf("failed to marshal node status response: %w", err)
	}
	var resp struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(b, &resp); err != nil {
		return "", fmt.Errorf("failed to unmarshal node status response: %w", err)
	}
	return resp.Status, nil
}

// CallSBCLI is a generic function to call the SimplyBlock API v2
func (client *RPCClient) CallSBCLI(method, path string, args interface{}) (interface{}, error) {
	path = strings.TrimLeft(path, "/")

	var bodyReader io.Reader
	if args != nil {
		data, err := json.Marshal(args)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", method, err)
		}
		bodyReader = bytes.NewReader(data)
	}

	requestURL := fmt.Sprintf("%s/%s", client.ClusterIP, path)
	klog.Infof("Calling Simplyblock API v2: %s %s", method, requestURL)

	req, err := http.NewRequest(method, requestURL, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", method, err)
	}

	req.Header.Set("Authorization", client.authorizationHeader(path))
	if args != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := client.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", method, err)
	}
	defer resp.Body.Close()

	// 204 No Content — success, no body
	if resp.StatusCode == http.StatusNoContent {
		return nil, nil
	}

	// 201 Created — return the UUID extracted from the Location header
	if resp.StatusCode == http.StatusCreated {
		location := resp.Header.Get("Location")
		if location == "" {
			return nil, fmt.Errorf("%s: 201 response missing Location header", method)
		}
		return locationToUUID(location), nil
	}

	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, fmt.Errorf("%s: failed to read response body: %w", method, readErr)
	}

	if resp.StatusCode >= http.StatusBadRequest {
		msg := extractErrorMessage(body)
		if msg == "" {
			msg = http.StatusText(resp.StatusCode)
		}
		return nil, fmt.Errorf("%s: %s", method, msg)
	}

	var result interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("%s: failed to decode response: %w", method, err)
	}
	return result, nil
}

func (client *RPCClient) authorizationHeader(path string) string {
	if strings.HasPrefix(path, "api/v2/") {
		return "Bearer " + client.ClusterSecret
	}
	return client.ClusterID + " " + client.ClusterSecret
}

// locationToUUID extracts the last path segment from a Location header value.
// e.g. "/clusters/x/storage-pools/y/volumes/uuid/" → "uuid"
func locationToUUID(location string) string {
	trimmed := strings.TrimRight(location, "/")
	idx := strings.LastIndex(trimmed, "/")
	if idx < 0 {
		return trimmed
	}
	return trimmed[idx+1:]
}

// extractErrorMessage pulls a human-readable message from a FastAPI error response body.
func extractErrorMessage(body []byte) string {
	var resp struct {
		Detail interface{} `json:"detail"`
		Error  string      `json:"error"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return string(body)
	}
	if resp.Detail != nil {
		switch v := resp.Detail.(type) {
		case string:
			return v
		default:
			b, _ := json.Marshal(v)
			return string(b)
		}
	}
	if resp.Error != "" {
		return resp.Error
	}
	return string(body)
}

// errorMatches checks if the error message from the full error
func errorMatches(errFull, errJSON error) bool {
	if errFull == nil {
		return false
	}
	strFull := strings.ToLower(errFull.Error())
	strJSON := strings.ToLower(errJSON.Error())
	strJSON = strings.TrimPrefix(strJSON, "json:")
	strJSON = strings.TrimSpace(strJSON)
	return strings.Contains(strFull, strJSON)
}
