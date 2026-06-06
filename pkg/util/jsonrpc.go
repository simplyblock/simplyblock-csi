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
	"context"
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

// errors deserve special care
var (
	ErrJSONNoSpaceLeft  = errors.New("json: No space left")
	ErrJSONNoSuchDevice = errors.New("json: No such device")
	ErrJSONVolumeExists = errors.New("json: Volume exists")

	// internal errors
	ErrVolumeDeleted     = errors.New("volume deleted")
	ErrVolumeUnpublished = errors.New("volume not published")
)

// ClusterAPI is the interface through which the CSI driver manages volumes,
// snapshots, and storage pools on a SimplyBlock cluster.
//
// Concurrency: CreateVolume is safe for concurrent calls. Publish/Unpublish/Delete
// for different volumes are safe concurrently; for the same volume they must be
// serialised by the caller.
//
// Idempotency: implementations must tolerate duplicate CSI requests (e.g. double
// publish) as required by the CSI spec.
type ClusterAPI interface {
	// Identity
	Info() string
	ClusterID() string
	PoolID() string

	// Storage pools
	ListStoragePools(ctx context.Context) ([]StoragePool, error)
	GetPoolUUIDByName(ctx context.Context, poolName string) (string, error)
	GetMasterLvols(ctx context.Context, poolUUID string) ([]MasterLvol, error)

	// Volumes
	CreateVolume(ctx context.Context, params *CreateLVolData) (string, error)
	GetVolumeSize(ctx context.Context, lvolID string) (string, error)
	ListVolumes(ctx context.Context) ([]*LvolResp, error)
	ResizeVolume(ctx context.Context, lvolID string, newSize int64) (bool, error)
	DeleteVolume(ctx context.Context, lvolID string) error
	PublishVolume(ctx context.Context, lvolID string) error
	UnpublishVolume(ctx context.Context, lvolID string) error
	VolumeInfo(ctx context.Context, lvolID string, hostNQN string) (map[string]string, error)
	CloneVolume(ctx context.Context, lvolID, cloneName, newSize, pvcName string) (string, error)

	// Snapshots
	CreateSnapshot(ctx context.Context, lvolID, snapshotName string) (string, error)
	ListSnapshots(ctx context.Context) ([]*SnapshotResp, error)
	DeleteSnapshot(ctx context.Context, snapshotID string) error
	CloneSnapshot(ctx context.Context, snapshotID, cloneName, newSize, pvcName string) (string, error)
}

// StoragePool represents a SimplyBlock storage pool returned by the cluster API.
type StoragePool struct {
	Name string `json:"name"`
	UUID string `json:"id"`
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

// Connection holds the shared HTTP transport to a webappapi service.
// One Connection serves multiple clusters that share the same endpoint.
type Connection struct {
	Endpoint string
	HTTP     *http.Client
}

// APIClient is cluster-scoped: it carries the credentials for one cluster
// and borrows a Connection to reach the webappapi service.
// It is immutable after construction; pool scoping is handled by the caller.
type APIClient struct {
	ClusterID     string
	ClusterSecret string
	conn          *Connection
}

// ClusterStatus is a partial view of the GET /clusters/{id}/ response in v2.
type ClusterStatus struct {
	Status string `json:"status"`
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

func (client APIClient) info() string {
	return client.ClusterID
}

// --- v2 URL path helpers ---

func (client APIClient) v2pools() string {
	return fmt.Sprintf("api/v2/clusters/%s/storage-pools/", client.ClusterID)
}

func (client APIClient) v2pool(poolID string) string {
	return fmt.Sprintf("api/v2/clusters/%s/storage-pools/%s", client.ClusterID, poolID)
}

func (client APIClient) v2volumes(poolID string) string {
	return fmt.Sprintf("api/v2/clusters/%s/storage-pools/%s/volumes/", client.ClusterID, poolID)
}

func (client APIClient) v2volume(poolID, volumeID string) string {
	return fmt.Sprintf("api/v2/clusters/%s/storage-pools/%s/volumes/%s/", client.ClusterID, poolID, volumeID)
}

func (client APIClient) v2snapshots(poolID string) string {
	return fmt.Sprintf("api/v2/clusters/%s/storage-pools/%s/snapshots/", client.ClusterID, poolID)
}

func (client APIClient) v2snapshot(poolID, snapshotID string) string {
	return fmt.Sprintf("api/v2/clusters/%s/storage-pools/%s/snapshots/%s/", client.ClusterID, poolID, snapshotID)
}

func (client APIClient) v2storageNode(nodeID string) string {
	return fmt.Sprintf("api/v2/clusters/%s/storage-nodes/%s/", client.ClusterID, nodeID)
}

// --- API methods ---

// listStoragePools returns all available storage pools
func (client APIClient) listStoragePools(ctx context.Context) ([]StoragePool, error) {
	raw, err := client.do(ctx, "GET", client.v2pools(), nil)
	if err != nil {
		return nil, err
	}
	var result []StoragePool
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal pools response: %w", err)
	}
	return result, nil
}

// createVolume creates a logical volume and returns its UUID
func (client APIClient) createVolume(ctx context.Context, poolID string, params *CreateLVolData) (string, error) {
	raw, err := client.do(ctx, "POST", client.v2volumes(poolID), params)
	if err != nil {
		if errorMatches(err, ErrJSONNoSpaceLeft) {
			err = ErrJSONNoSpaceLeft
		} else if strings.Contains(strings.ToLower(err.Error()), "exists") || strings.Contains(strings.ToLower(err.Error()), "conflict") {
			err = ErrJSONVolumeExists
		}
		return "", err
	}
	var lvolID string
	if err := json.Unmarshal(raw, &lvolID); err != nil {
		return "", fmt.Errorf("unexpected response for createVolume: %w", err)
	}
	return lvolID, nil
}

// getVolumeByUUID fetches a single volume by its UUID
func (client APIClient) getVolumeByUUID(ctx context.Context, poolID, lvolID string) (*LvolResp, error) {
	raw, err := client.do(ctx, "GET", client.v2volume(poolID, lvolID), nil)
	if err != nil {
		if errorMatches(err, ErrJSONNoSuchDevice) {
			err = ErrJSONNoSuchDevice
		}
		return nil, err
	}
	var result LvolResp
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal volume response: %w", err)
	}
	return &result, nil
}

// getVolume accepts either a UUID or a "poolName/volName" path (legacy callers)
func (client APIClient) getVolume(ctx context.Context, poolID, lvolID string) (*LvolResp, error) {
	return client.getVolumeByUUID(ctx, poolID, lvolID)
}

// listVolumes returns all volumes in the pool
func (client APIClient) listVolumes(ctx context.Context, poolID string) ([]*LvolResp, error) {
	raw, err := client.do(ctx, "GET", client.v2volumes(poolID), nil)
	if err != nil {
		return nil, err
	}
	var results []*LvolResp
	if err := json.Unmarshal(raw, &results); err != nil {
		return nil, fmt.Errorf("failed to unmarshal volumes response: %w", err)
	}
	return results, nil
}

// getVolumeInfo returns the NVMe connection info for a volume
func (client APIClient) getVolumeInfo(ctx context.Context, poolID, lvolID, hostNQN string) (map[string]string, error) {
	result, err := client.getLvolConnections(ctx, poolID, lvolID, hostNQN)
	if err != nil {
		if errorMatches(err, ErrJSONNoSuchDevice) {
			err = ErrJSONNoSuchDevice
		}
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
func (client APIClient) deleteVolume(ctx context.Context, poolID, lvolID string) error {
	_, err := client.do(ctx, "DELETE", client.v2volume(poolID, lvolID), nil)
	if err != nil && (errorMatches(err, ErrJSONNoSuchDevice) || strings.Contains(err.Error(), "404")) {
		err = ErrJSONNoSuchDevice
	}
	return err
}

// getPoolUUIDByName resolves a pool name to its UUID
func (client APIClient) getPoolUUIDByName(ctx context.Context, poolName string) (string, error) {
	pools, err := client.listStoragePools(ctx)
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
func (client APIClient) getMasterLvols(ctx context.Context, poolUUID string) ([]MasterLvol, error) {
	path := client.v2pool(poolUUID) + "/master-lvols"
	raw, err := client.do(ctx, "GET", path, nil)
	if err != nil {
		return nil, err
	}
	if raw == nil {
		return []MasterLvol{}, nil
	}
	var result []MasterLvol
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal master lvols response: %w", err)
	}
	if result == nil {
		return []MasterLvol{}, nil
	}
	return result, nil
}

// resizeVolume resizes a volume
func (client APIClient) resizeVolume(ctx context.Context, poolID, lvolID string, size int64) (bool, error) {
	_, err := client.do(ctx, "PUT", client.v2volume(poolID, lvolID), &ResizeVolReq{Size: size})
	if err != nil {
		return false, err
	}
	return true, nil
}

// cloneVolume clones a volume by UUID, returning the new volume's UUID
func (client APIClient) cloneVolume(ctx context.Context, poolID, lvolID, cloneName, newSize, pvcName string) (string, error) {
	path := client.v2volume(poolID, lvolID) + "clone?clone_name=" + url.QueryEscape(cloneName)
	if newSize != "" {
		path += "&new_size=" + url.QueryEscape(newSize)
	}
	if pvcName != "" {
		path += "&pvc_name=" + url.QueryEscape(pvcName)
	}

	klog.V(5).Infof("cloneVolume size: %s", newSize)

	raw, err := client.do(ctx, "POST", path, nil)
	if err != nil {
		if errorMatches(err, ErrJSONNoSpaceLeft) {
			err = ErrJSONNoSpaceLeft
		}
		return "", err
	}
	var lvID string
	if err := json.Unmarshal(raw, &lvID); err != nil {
		return "", fmt.Errorf("unexpected response for cloneVolume: %w", err)
	}
	return lvID, nil
}

// cloneSnapshot creates a new volume from a snapshot, returning the new volume's UUID
func (client APIClient) cloneSnapshot(ctx context.Context, poolID, snapshotID, cloneName, newSize, pvcName string) (string, error) {
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

	raw, err := client.do(ctx, "POST", client.v2volumes(poolID), &params)
	if err != nil {
		if errorMatches(err, ErrJSONNoSpaceLeft) {
			err = ErrJSONNoSpaceLeft
		}
		return "", err
	}
	var lvolID string
	if err := json.Unmarshal(raw, &lvolID); err != nil {
		return "", fmt.Errorf("unexpected response for cloneSnapshot: %w", err)
	}
	return lvolID, nil
}

// listSnapshots returns all snapshots in the pool
func (client APIClient) listSnapshots(ctx context.Context, poolID string) ([]*SnapshotResp, error) {
	raw, err := client.do(ctx, "GET", client.v2snapshots(poolID), nil)
	if err != nil {
		return nil, err
	}
	var results []*SnapshotResp
	if err := json.Unmarshal(raw, &results); err != nil {
		return nil, fmt.Errorf("failed to unmarshal snapshots response: %w", err)
	}
	return results, nil
}

// listAllSnapshots iterates every pool in the cluster and collects all snapshots.
// Used when no pool is set (e.g. ListSnapshots CSI RPC).
func (client APIClient) listAllSnapshots(ctx context.Context) ([]*SnapshotResp, error) {
	pools, err := client.listStoragePools(ctx)
	if err != nil {
		return nil, err
	}
	var all []*SnapshotResp
	for _, pool := range pools {
		snaps, err := client.listSnapshots(ctx, pool.UUID)
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
func (client APIClient) snapshot(ctx context.Context, poolID, lvolID, snapShotName string) (string, error) {
	params := struct {
		Name string `json:"name"`
	}{Name: snapShotName}

	path := client.v2volume(poolID, lvolID) + "snapshots"
	raw, err := client.do(ctx, "POST", path, &params)
	if err != nil {
		if errorMatches(err, ErrJSONNoSpaceLeft) {
			err = ErrJSONNoSpaceLeft
		}
		return "", err
	}
	var snapshotID string
	if err := json.Unmarshal(raw, &snapshotID); err != nil {
		return "", fmt.Errorf("unexpected response for snapshot: %w", err)
	}
	return snapshotID, nil
}

// deleteSnapshot deletes a snapshot.
// If poolID is empty (legacy 2-part snapshot IDs), it scans all pools.
func (client APIClient) deleteSnapshot(ctx context.Context, poolID, snapshotID string) error {
	if poolID == "" {
		return client.deleteSnapshotScanPools(ctx, snapshotID)
	}
	_, err := client.do(ctx, "DELETE", client.v2snapshot(poolID, snapshotID), nil)
	if err != nil && (errorMatches(err, ErrJSONNoSuchDevice) || strings.Contains(err.Error(), "404")) {
		err = ErrJSONNoSuchDevice
	}
	return err
}

// deleteSnapshotScanPools finds a snapshot across all pools and deletes it.
// Used for backward compat with legacy 2-part CSI snapshot IDs that have no pool info.
func (client APIClient) deleteSnapshotScanPools(ctx context.Context, snapshotID string) error {
	pools, err := client.listStoragePools(ctx)
	if err != nil {
		return fmt.Errorf("failed to list pools while deleting snapshot %s: %w", snapshotID, err)
	}
	for _, pool := range pools {
		_, err := client.do(ctx, "DELETE", client.v2snapshot(pool.UUID, snapshotID), nil)
		if err == nil {
			return nil
		}
		if !strings.Contains(err.Error(), "404") && !strings.Contains(err.Error(), "Not Found") {
			return err
		}
	}
	return fmt.Errorf("snapshot %s not found in any pool", snapshotID)
}

// findPoolForVolume scans all storage pools to find which one contains the given
// volume UUID, and returns that pool's UUID. Returns an error if not found.
func (client APIClient) findPoolForVolume(ctx context.Context, lvolID string) (string, error) {
	pools, err := client.listStoragePools(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to list pools for volume %s: %w", lvolID, err)
	}
	for _, pool := range pools {
		_, err := client.getVolumeByUUID(ctx, pool.UUID, lvolID)
		if err == nil {
			return pool.UUID, nil
		}
		// FIXME: instead of parsing error messages, use an HTTP error type with status code
		if !errorMatches(err, ErrJSONNoSuchDevice) && !strings.Contains(err.Error(), "404") {
			return "", fmt.Errorf("unexpected error searching for volume %s in pool %s: %w", lvolID, pool.UUID, err)
		}
	}
	return "", fmt.Errorf("volume %s not found in any pool", lvolID)
}

// getLvolConnections returns the raw NVMe-oF connection list for a volume.
func (client APIClient) getLvolConnections(ctx context.Context, poolID, lvolID, hostNQN string) ([]*LvolConnectResp, error) {
	path := client.v2volume(poolID, lvolID) + "connect"
	if hostNQN != "" {
		path += "?host_nqn=" + url.QueryEscape(hostNQN)
	}
	raw, err := client.do(ctx, "GET", path, nil)
	if err != nil {
		return nil, err
	}
	var result []*LvolConnectResp
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal connections response: %w", err)
	}
	return result, nil
}

// getStorageNodeStatus returns the status string for a storage node by UUID.
func (client APIClient) getStorageNodeStatus(ctx context.Context, nodeID string) (string, error) {
	raw, err := client.do(ctx, "GET", client.v2storageNode(nodeID), nil)
	if err != nil {
		return "", err
	}
	var resp struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return "", fmt.Errorf("failed to unmarshal node status response: %w", err)
	}
	return resp.Status, nil
}

// do executes an HTTP request against the SimplyBlock API and returns the raw
// JSON response body. Callers unmarshal directly into their typed structs.
//
// Response handling:
//   - 204 No Content → (nil, nil)
//   - 201 Created    → UUID extracted from Location header, encoded as a JSON string
//   - 2xx            → raw JSON body
//   - 4xx/5xx        → error with extracted message
func (client APIClient) do(ctx context.Context, method, path string, body any) (json.RawMessage, error) {
	path = strings.TrimLeft(path, "/")

	var bodyReader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", method, err)
		}
		bodyReader = bytes.NewReader(data)
	}

	requestURL := fmt.Sprintf("%s/%s", client.conn.Endpoint, path)
	klog.Infof("Calling Simplyblock API v2: %s %s", method, requestURL)

	req, err := http.NewRequestWithContext(ctx, method, requestURL, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", method, err)
	}

	req.Header.Set("Authorization", client.authorizationHeader(path))
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := client.conn.HTTP.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", method, err)
	}
	defer resp.Body.Close()

	// 204 No Content — success, no body
	if resp.StatusCode == http.StatusNoContent {
		return nil, nil
	}

	// 201 Created — return the UUID extracted from the Location header, encoded as a JSON string
	if resp.StatusCode == http.StatusCreated {
		location := resp.Header.Get("Location")
		if location == "" {
			return nil, fmt.Errorf("%s: 201 response missing Location header", method)
		}
		encoded, _ := json.Marshal(locationToUUID(location))
		return json.RawMessage(encoded), nil
	}

	raw, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, fmt.Errorf("%s: failed to read response body: %w", method, readErr)
	}

	if resp.StatusCode >= http.StatusBadRequest {
		msg := extractErrorMessage(raw)
		if msg == "" {
			msg = http.StatusText(resp.StatusCode)
		}
		return nil, fmt.Errorf("%s %d: %s", method, resp.StatusCode, msg)
	}

	return json.RawMessage(raw), nil
}

func (client APIClient) authorizationHeader(path string) string {
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
		Detail any    `json:"detail"`
		Error  string `json:"error"`
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
