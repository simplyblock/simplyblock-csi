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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net"

	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"k8s.io/klog"
)

const (
	// DevDiskByID is the path to the device file under /dev/disk/by-id
	DevDiskByID = "/dev/disk/by-id/*%s*"

	// TargetTypeNVMf is the target type for NVMe over Fabrics
	TargetTypeTCP  = "tcp"
	TargetTypeRDMA = "rdma"

	// DefaultCtrlLossTmo is the NVMe-oF controller loss timeout in seconds.
	DefaultCtrlLossTmo = 60
)

// SpdkCsiInitiator defines interface for NVMeoF/iSCSI initiator
//   - Connect initiates target connection and returns local block device filename
//     e.g., /dev/disk/by-id/nvme-SPDK_Controller1_SPDK00000000000001
//   - Disconnect terminates target connection
//   - Caller(node service) should serialize calls to same initiator
//   - Implementation should be idempotent to duplicated requests
type SpdkCsiInitiator interface {
	Connect(ctx context.Context) (string, error)
	Disconnect(ctx context.Context) error
}

// initiatorNVMf is an implementation of NVMf tcp initiator
type initiatorNVMf struct {
	lvolID         string
	targetType     string
	nqn            string
	reconnectDelay string
	nrIoQueues     string
	ctrlLossTmo    string
	model          string
	nsId           string
	hostIface      string
	hostNQN        string
	poolID         string
}

type path struct {
	Name      string `json:"Name"`
	Transport string `json:"Transport"`
	Address   string `json:"Address"`
	State     string `json:"State"`
	ANAState  string `json:"ANAState"`
}

type subsystem struct {
	Name  string `json:"Name"`
	NQN   string `json:"NQN"`
	Paths []path `json:"Paths"`
}

type subsystemResponse struct {
	Subsystems []subsystem `json:"Subsystems"`
}

type NodeInfo struct {
	NodeID string   `json:"storage_node_id"` // v2 VolumeDTO field
	Nodes  []string `json:"nodes"`           // URL paths in v2; converted to UUIDs after parsing
	Status string   `json:"status"`
}

type nvmeDeviceInfo struct {
	devicePath   string
	serialNumber string
	lvolID       string // UUID from /sys/block/<dev>/uuid — set for namespaced LVols
}

var (
	devicePresentMap  = make(map[string]bool)
	deviceToLvolIDMap = make(map[string]string)
	mu                sync.Mutex

	// maxSeenPathsMap caches the highest number of active NVMe-oF paths ever
	// observed per NQN. Used by the connection monitor to detect degradation
	// without querying the API on every cycle.
	maxSeenPathsMap = make(map[string]int)
	maxSeenMu       sync.Mutex
)

// clusterConfig represents the Kubernetes secret structure
type ClusterConfig struct {
	ClusterID       string `json:"cluster_id"`
	ClusterEndpoint string `json:"cluster_endpoint"`
	ClusterSecret   string `json:"cluster_secret"`
}

type ClustersInfo struct {
	Clusters []ClusterConfig `json:"clusters"`
}

// NewsimplyBlockClient creates a new Simplyblock client scoped to a cluster and optionally a pool.
// poolIDOrName may be a pool UUID (used as-is) or a pool name (resolved via API), or empty
// (no pool context — only cluster-level operations will work).
func NewsimplyBlockClient(ctx context.Context, clusterID, poolIDOrName string) (*ClusterClient, error) {
	secretFile := FromEnv("SPDKCSI_SECRET", "/etc/spdkcsi-secret/secret.json")
	var clusters ClustersInfo
	err := ParseJSONFile(secretFile, &clusters)
	if err != nil {
		return nil, fmt.Errorf("failed to parse secret file: %w", err)
	}

	var clusterConfig *ClusterConfig
	for _, cluster := range clusters.Clusters {
		if cluster.ClusterID == clusterID {
			clusterConfig = &cluster
			break
		}
	}

	if clusterConfig == nil {
		return nil, fmt.Errorf("failed to find secret for clusterID %s", clusterID)
	}

	if clusterConfig.ClusterEndpoint == "" || clusterConfig.ClusterSecret == "" {
		return nil, fmt.Errorf("invalid cluster configuration for clusterID %s", clusterID)
	}

	klog.Infof("Simplyblock client created for ClusterID:%s, Endpoint:%s",
		clusterConfig.ClusterID,
		clusterConfig.ClusterEndpoint,
	)

	conn, err := NewConnection(clusterConfig.ClusterEndpoint)
	if err != nil {
		return nil, err
	}
	c := &ClusterClient{
		API: &APIClient{
			ClusterID:     clusterID,
			ClusterSecret: clusterConfig.ClusterSecret,
			conn:          conn,
		},
	}

	if poolIDOrName != "" {
		poolUUID, err := resolvePoolUUID(ctx, c, poolIDOrName)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve pool %q: %w", poolIDOrName, err)
		}
		c.poolID = poolUUID
	}

	return c, nil
}

// resolvePoolUUID returns poolIDOrName as-is if it is already a UUID,
// otherwise looks up the pool UUID by name via the API.
func resolvePoolUUID(ctx context.Context, c *ClusterClient, poolIDOrName string) (string, error) {
	if isUUID(poolIDOrName) {
		return poolIDOrName, nil
	}
	return c.GetPoolUUIDByName(ctx, poolIDOrName)
}

// isUUID reports whether s is a standard UUID (8-4-4-4-12 hex, with hyphens).
func isUUID(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i, c := range s {
		switch i {
		case 8, 13, 18, 23:
			if c != '-' {
				return false
			}
		default:
			if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
				return false
			}
		}
	}
	return true
}

// NewSpdkCsiInitiator creates a new SpdkCsiInitiator based on the target type
func NewSpdkCsiInitiator(volumeContext map[string]string) (SpdkCsiInitiator, error) {
	targetType := strings.ToLower(volumeContext["targetType"])
	klog.Infof("Simplyblock targetType created :%s", targetType)
	switch targetType {
	case TargetTypeTCP, TargetTypeRDMA:
		return &initiatorNVMf{
			targetType:     volumeContext["targetType"],
			nqn:            volumeContext["nqn"],
			reconnectDelay: volumeContext["reconnectDelay"],
			nrIoQueues:     volumeContext["nrIoQueues"],
			ctrlLossTmo:    volumeContext["ctrlLossTmo"],
			model:          volumeContext["model"],
			nsId:           volumeContext["nsId"],
			hostIface:      volumeContext["hostIface"],
			hostNQN:        volumeContext["hostNQN"],
			poolID:         volumeContext["poolID"],
			lvolID:         volumeContext["uuid"],
		}, nil

	default:
		return nil, fmt.Errorf("unknown initiator: %s", targetType)
	}
}

func execWithTimeoutRetry(ctx context.Context, cmdLine []string, timeout, retry int) (err error) {
	for retry > 0 {
		err = execWithTimeout(ctx, cmdLine, timeout)
		if err == nil {
			return nil
		}
		retry--
	}
	return err
}

func (nvmf *initiatorNVMf) Connect(ctx context.Context) (string, error) {
	alreadyConnected, err := isNqnConnected(nvmf.nqn)
	if err != nil {
		klog.Errorf("Failed to check existing connections: %v", err)
		return "", err
	}

	if !alreadyConnected {
		clusterID, _ := getLvolIDFromNQN(nvmf.nqn)
		// the lvolID from NQN gives the master LvolID of the subsystem
		// Although the connection string is same for all the lvols in the subsystem,
		// volume/<lvol-id>/connect/ connect API return 404 if master lvol is deleted
		// so using the actual lvolID instead instead of master lvol ID
		lvolID := nvmf.lvolID
		sbcClient, err := NewsimplyBlockClient(ctx, clusterID, nvmf.poolID)
		if err != nil {
			klog.Errorf("failed to create SPDK client: %v", err)
			return "", err
		}
		connections, err := fetchLvolConnection(ctx, sbcClient, lvolID, nvmf.hostNQN)
		if err != nil {
			klog.Errorf("Failed to get lvol connection: %v", err)
			return "", err
		}

		ctrlLossTmo := DefaultCtrlLossTmo

		connected := 0
		var lastErr error

		for _, conn := range connections {
			err := connectViaNVMe(ctx, conn, ctrlLossTmo, len(connections))
			if err != nil {
				klog.Errorf("nvme connect failed for %s:%d: %v", conn.IP, conn.Port, err)
				lastErr = err
				continue
			}
			connected++
		}
		if connected == 0 {
			return "", fmt.Errorf(
				"failed to connect to any NVMe path for NQN %s: error: %v",
				nvmf.nqn, lastErr,
			)
		}
	}

	deviceGlob := fmt.Sprintf(DevDiskByID, fmt.Sprintf("%s*_%s", nvmf.model, nvmf.nsId))

	deviceGlobOld := fmt.Sprintf(DevDiskByID, nvmf.model)

	deviceGlobFallback := fmt.Sprintf(DevDiskByID, fmt.Sprintf("%s*_%s", nvmf.lvolID, nvmf.nsId))

	devicePath, err := waitForDeviceReady(ctx, deviceGlob, 10)
	if err != nil {
		klog.Warningf("New device symlink not found (%s). Retrying legacy format: %s", deviceGlob, deviceGlobOld)
		devicePath, err = waitForDeviceReady(ctx, deviceGlobOld, 10)
		if err != nil {
			klog.Warningf("Legacy format not found (%s). Retrying with fallback: %s", deviceGlobOld, deviceGlobFallback)
			devicePath, err = waitForDeviceReady(ctx, deviceGlobFallback, 10)
			if err != nil {
				return "", fmt.Errorf("device not found in both new (%s), old (%s), and fallback (%s) formats: %w",
					deviceGlob, deviceGlobOld, deviceGlobFallback, err)
			}
		}
	}
	return devicePath, nil
}

func (nvmf *initiatorNVMf) Disconnect(ctx context.Context) error {
	//deviceGlob := fmt.Sprintf(DevDiskByID, nvmf.model)
	deviceGlob := fmt.Sprintf(DevDiskByID, fmt.Sprintf("%s*_[0-9]*", nvmf.model))
	devicePath, err := filepath.Glob(deviceGlob)
	if err != nil {
		return fmt.Errorf("failed to find device paths matching %s: %v", deviceGlob, err)
	}

	if len(devicePath) > 1 {
		return nil

	} else if len(devicePath) == 1 {
		err = disconnectDevicePath(ctx, devicePath[0])

		if err != nil {
			return err
		}
	}

	return waitForDeviceGone(deviceGlob)
}

// when timeout is set as 0, try to find the device file immediately
// otherwise, wait for device file comes up or timeout.
func waitForDeviceReady(ctx context.Context, deviceGlob string, seconds int) (string, error) {
	for i := 0; i <= seconds; i++ {
		matches, err := filepath.Glob(deviceGlob)
		if err != nil {
			return "", err
		}
		// two symbol links under /dev/disk/by-id/ to same device
		if len(matches) >= 1 {
			return matches[0], nil
		}
		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	return "", fmt.Errorf("timed out waiting device ready: %s", deviceGlob)
}

// wait for device file gone or timeout
func waitForDeviceGone(deviceGlob string) error {
	for i := 0; i <= 20; i++ {
		matches, err := filepath.Glob(deviceGlob)
		if err != nil {
			return err
		}
		if len(matches) == 0 {
			return nil
		}
		time.Sleep(time.Second)
	}
	return fmt.Errorf("timed out waiting device gone: %s", deviceGlob)
}

// exec shell command with timeout(in seconds)
func execWithTimeout(ctx context.Context, cmdLine []string, timeout int) error {
	execCtx, cancel := context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
	defer cancel()

	klog.Infof("running command: %v", cmdLine)
	//nolint:gosec // execWithTimeout assumes valid cmd arguments
	cmd := exec.CommandContext(execCtx, cmdLine[0], cmdLine[1:]...)
	output, err := cmd.CombinedOutput()

	if errors.Is(execCtx.Err(), context.DeadlineExceeded) {
		return errors.New("timed out")
	}
	if output != nil {
		klog.Infof("command returned: %s", output)
	}
	if err != nil && len(output) > 0 {
		return fmt.Errorf("%w: %s", err, strings.TrimSpace(string(output)))
	}
	return err
}

func disconnectDevicePath(ctx context.Context, devicePath string) error {
	var paths []path

	realPath, err := filepath.EvalSymlinks(devicePath)
	if err != nil {
		return fmt.Errorf("Failed to resolve device path from %s: %v", devicePath, err)
	}

	subsystems, err := getSubsystemsForDevice(realPath)
	if err != nil {
		return fmt.Errorf("Failed to get subsystems for %s: %v", realPath, err)
	}

	for _, host := range subsystems {
		for _, subsystem := range host.Subsystems {
			for _, p := range subsystem.Paths {
				paths = append(paths, path{
					Name:     p.Name,
					ANAState: p.ANAState,
				})
			}
		}
	}

	sort.Slice(paths, func(i, j int) bool {
		if paths[i].ANAState == "optimized" && paths[j].ANAState != "optimized" {
			return false
		}
		return true
	})

	for _, p := range paths {
		klog.Infof("Disconnecting device %s", p.Name)
		disconnectCmd := []string{"nvme", "disconnect", "-d", p.Name}
		err := execWithTimeoutRetry(ctx, disconnectCmd, 40, 1)
		if err != nil {
			klog.Errorf("Failed to disconnect device %s: %v", p.Name, err)
		}
	}

	mu.Lock()
	delete(devicePresentMap, realPath)
	delete(deviceToLvolIDMap, realPath)
	mu.Unlock()

	return nil
}

// logicalVolumeIdByDevicePath reads /sys/block/<dev>/uuid for a device path like /dev/nvme0n2.
// Returns an empty string if the file is absent, unreadable, or not a valid UUID.
func logicalVolumeIdByDevicePath(devicePath string) string {
	name := filepath.Base(devicePath)
	data, err := os.ReadFile(filepath.Join("/sys/block", name, "uuid"))
	if err != nil {
		return ""
	}
	uuid := strings.TrimSpace(string(data))
	if !isUUID(uuid) {
		return ""
	}
	return uuid
}

func getNVMeDeviceInfos() ([]nvmeDeviceInfo, error) {
	cmd := exec.Command("nvme", "list", "-o", "json")
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("failed to execute nvme list: %v", err)
	}

	var deviceResponse struct {
		Devices []struct {
			Subsystems []struct {
				Namespaces []struct {
					NameSpace string `json:"NameSpace"`
				} `json:"Namespaces"`
			} `json:"Subsystems"`
		} `json:"Devices"`
	}
	if err := json.Unmarshal(output, &deviceResponse); err == nil {
		var devices []nvmeDeviceInfo
		for _, host := range deviceResponse.Devices {
			for _, sub := range host.Subsystems {
				for _, ns := range sub.Namespaces {
					if ns.NameSpace == "" {
						continue
					}
					dp := "/dev/" + ns.NameSpace
					devices = append(devices, nvmeDeviceInfo{
						devicePath: dp,
						lvolID:     logicalVolumeIdByDevicePath(dp),
					})
				}
			}
		}
		if len(devices) > 0 {
			return devices, nil
		}
	}

	// Legacy flat format: Devices[].DevicePath
	var legacyDeviceResp struct {
		Devices []struct {
			DevicePath   string `json:"DevicePath"`
			SerialNumber string `json:"SerialNumber"`
		} `json:"Devices"`
	}
	if err := json.Unmarshal(output, &legacyDeviceResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal nvme list output: %v", err)
	}
	var devices []nvmeDeviceInfo
	for _, dev := range legacyDeviceResp.Devices {
		if dev.DevicePath == "" {
			continue
		}
		devices = append(devices, nvmeDeviceInfo{
			devicePath:   dev.DevicePath,
			serialNumber: dev.SerialNumber,
			lvolID:       logicalVolumeIdByDevicePath(dev.DevicePath),
		})
	}
	return devices, nil
}

func isNqnConnected(nqn string) (bool, error) {
	cmd := exec.Command("nvme", "list-subsys")
	output, err := cmd.Output()
	if err != nil {
		return false, fmt.Errorf("failed to execute nvme list-subsys: %v", err)
	}

	lines := strings.Split(string(output), "\n")
	for _, line := range lines {
		if strings.Contains(line, nqn) {
			parts := strings.Fields(line)
			if len(parts) > 0 {
				return true, nil
			}
		}
	}
	return false, nil
}

func getSubsystemsForDevice(devicePath string) ([]subsystemResponse, error) {
	cmd := exec.Command("nvme", "list-subsys", "-o", "json", devicePath)
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("failed to execute nvme list-subsys: %v", err)
	}

	var subsystems []subsystemResponse
	if err := json.Unmarshal(output, &subsystems); err != nil {
		return nil, fmt.Errorf("failed to unmarshal nvme list-subsys output: %v", err)
	}

	return subsystems, nil
}

func getLvolIDFromNQN(nqn string) (clusterID, lvolID string) {
	parts := strings.Split(nqn, ":lvol:")
	if len(parts) > 1 {
		subparts := strings.Split(parts[0], ":")
		clusterID := subparts[len(subparts)-1]
		lvolID := parts[1]
		return clusterID, lvolID
	}
	return "", ""
}

func parseAddress(address string) string {
	parts := strings.Split(address, ",")
	for _, part := range parts {
		if strings.HasPrefix(part, "traddr=") {
			return strings.TrimPrefix(part, "traddr=")
		}
	}
	return ""
}

func reconnectSubsystems(markBroken func(lvolID string)) error {
	devices, err := getNVMeDeviceInfos()
	if err != nil {
		return fmt.Errorf("failed to get NVMe device paths: %v", err)
	}

	currentDevices := make(map[string]bool)

	for _, device := range devices {
		subsystems, err := getSubsystemsForDevice(device.devicePath)
		if err != nil {
			klog.Errorf("failed to get subsystems for device %s: %v", device.devicePath, err)
			continue
		}

		currentDevices[device.devicePath] = true

		for _, host := range subsystems {
			for _, subsystem := range host.Subsystems {
				clusterID, nqnLvolID := getLvolIDFromNQN(subsystem.NQN)
				if nqnLvolID == "" {
					continue
				}
				// Prefer the sysfs UUID when available — it always identifies the
				// exact namespace LVol. Falls back to the NQN-derived ID.
				lvolID := device.lvolID
				if lvolID == "" {
					lvolID = nqnLvolID
				}

				// Only mark the device present once we have a confirmed lvolID,
				// so the cleanup loop never sees a device without a mapping.
				mu.Lock()
				devicePresentMap[device.devicePath] = true
				deviceToLvolIDMap[device.devicePath] = lvolID
				mu.Unlock()

				numActive := len(subsystem.Paths)
				if numActive == 0 {
					continue
				}

				expected := resolveExpectedPathCount(subsystem.NQN, clusterID, lvolID, numActive)

				needsRecovery := numActive < expected ||
					(expected > 1 && hasConnectingPath(subsystem.Paths))

				if !needsRecovery {
					continue
				}

				if !confirmSubsystemNeedsRecovery(&subsystem, device.devicePath, numActive) {
					continue
				}

				klog.Infof("Degraded subsystem: NQN=%s active=%d expected=%d device=%s",
					subsystem.NQN, numActive, expected, device.devicePath)

				if err := recoverPathsWithANA(clusterID, lvolID, device.devicePath, subsystem.Paths); err != nil {
					klog.Errorf("failed to recover paths for lvolID %s: %v", lvolID, err)
				}
			}
		}
	}

	var goneLvols []string

	mu.Lock()
	for devPath := range devicePresentMap {
		if !currentDevices[devPath] {
			lvolID := deviceToLvolIDMap[devPath]
			klog.Errorf("Device %s is no longer present — all NVMe-oF connections were lost and the kernel removed the device (lvolID=%s)", devPath, lvolID)
			delete(devicePresentMap, devPath)
			delete(deviceToLvolIDMap, devPath)
			if lvolID != "" {
				goneLvols = append(goneLvols, lvolID)
			}
		}
	}
	mu.Unlock()

	if markBroken != nil {
		for _, lvolID := range goneLvols {
			markBroken(lvolID)
		}
	}

	return nil
}

func fetchNodeInfo(ctx context.Context, client *ClusterClient, lvolID string) (*NodeInfo, error) {
	poolID, err := client.poolForVolume(ctx, lvolID)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve pool for volume %s: %w", lvolID, err)
	}
	raw, err := client.API.do(ctx, "GET", client.API.v2volume(poolID, lvolID), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch node info: %w", err)
	}
	var info NodeInfo
	if err := json.Unmarshal(raw, &info); err != nil {
		return nil, fmt.Errorf("failed to unmarshal node info: %w", err)
	}
	// v2 nodes field returns URL paths; extract UUIDs from last path segment
	for i, n := range info.Nodes {
		info.Nodes[i] = locationToUUID(n)
	}
	return &info, nil
}

func isAnyConnReachable(ctx context.Context, conns []*LvolConnectResp) bool {
	for _, conn := range conns {
		if isTCPReachable(ctx, conn.IP, conn.Port) {
			return true
		}
	}
	return false
}

func isTCPReachable(ctx context.Context, ip string, port int) bool {
	d := net.Dialer{Timeout: 1 * time.Second}
	conn, err := d.DialContext(ctx, "tcp", fmt.Sprintf("%s:%d", ip, port))
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

func isNodeOnline(ctx context.Context, client *ClusterClient, nodeID, ip string, port int) bool {
	status, err := client.API.getStorageNodeStatus(ctx, nodeID)
	if err != nil {
		klog.Errorf("failed to fetch node status for node %s: %v", nodeID, err)
		return false
	}
	if status != "online" {
		return false
	}
	if ip != "" && port != 0 {
		if !isTCPReachable(ctx, ip, port) {
			klog.Infof("isNodeOnline: node %s API online but %s:%d not TCP-reachable", nodeID, ip, port)
			return false
		}
	}
	return true
}

func fetchLvolConnection(ctx context.Context, client *ClusterClient, lvolID string, hostNQN string) ([]*LvolConnectResp, error) {
	poolID, err := client.poolForVolume(ctx, lvolID)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve pool for volume %s: %w", lvolID, err)
	}
	connections, err := client.API.getLvolConnections(ctx, poolID, lvolID, hostNQN)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch connection: %w", err)
	}
	if len(connections) == 0 {
		return nil, fmt.Errorf("empty connection response for volume %s", lvolID)
	}
	return connections, nil
}

func connectViaNVMe(ctx context.Context, conn *LvolConnectResp, ctrlLossTmo int, retries int) error {
	cmd := []string{
		"nvme", "connect", "-t", strings.ToLower(conn.TargetType),
		"-a", conn.IP, "-s", strconv.Itoa(conn.Port),
		"-n", conn.Nqn,
		"-l", strconv.Itoa(ctrlLossTmo),
		"-c", strconv.Itoa(conn.ReconnectDelay),
		"-i", strconv.Itoa(conn.NrIoQueues),
	}
	if conn.HostIface != "" {
		cmd = append(cmd, "-f", conn.HostIface)
	}
	if err := execWithTimeoutRetry(ctx, cmd, 40, retries); err != nil {
		klog.Errorf("nvme connect failed: %v", err)
		return err
	}
	return nil
}

// confirmSubsystemNeedsRecovery re-checks the subsystem 5 times over 5 seconds
// and returns true only if the path count remained stable at initialPathCount for
// all 5 checks. This debounces spurious triggers during normal ANA switchovers.
func confirmSubsystemNeedsRecovery(subsystem *subsystem, devicePath string, initialPathCount int) bool {
	for i := 0; i < 5; i++ {
		recheck, err := getSubsystemsForDevice(devicePath)
		if err != nil {
			klog.Errorf("failed to recheck subsystems for device %s: %v", devicePath, err)
			continue
		}

		found := false
		for _, h := range recheck {
			for _, s := range h.Subsystems {
				if s.NQN == subsystem.NQN {
					found = true
					if len(s.Paths) != initialPathCount {
						return false
					}
				}
			}
		}

		if !found {
			klog.Warningf("Subsystem %s not found during recheck, assuming it's gone", subsystem.NQN)
			return false
		}

		time.Sleep(1 * time.Second)
	}
	return true
}

// MonitorConnection monitors NVMe-oF connections and reconnects missing or
// IP-changed paths. Supports 1-path, 2-path, and 3-path volumes
// (1 optimized + up to 2 non-optimized).
const (
	monitorBaseInterval    = 3 * time.Second
	monitorJitter          = 500 * time.Millisecond
	monitorMaxBackoff      = 60 * time.Second
	monitorCircuitAfter    = 5
	monitorCircuitCooldown = 30 * time.Second
)

func MonitorConnection(markBroken func(lvolID string)) {
	var (
		consecutiveErrors int
		backoff           = monitorBaseInterval
	)

	for {
		err := reconnectSubsystems(markBroken)
		if err != nil {
			consecutiveErrors++
			klog.Errorf("MonitorConnection error (%d consecutive): %v", consecutiveErrors, err)

			if consecutiveErrors >= monitorCircuitAfter {
				klog.Warningf("MonitorConnection: circuit open after %d failures, cooling down for %s", consecutiveErrors, monitorCircuitCooldown)
				time.Sleep(monitorCircuitCooldown)
				continue
			}

			// exponential backoff capped at monitorMaxBackoff
			backoff *= 2
			if backoff > monitorMaxBackoff {
				backoff = monitorMaxBackoff
			}
		} else {
			consecutiveErrors = 0
			backoff = monitorBaseInterval
		}

		jitter := time.Duration(rand.Int63n(int64(monitorJitter)))
		time.Sleep(backoff + jitter)
	}
}

// hasConnectingPath reports whether any path has State == "connecting".
// On a multi-path volume this typically means a node's IP changed and the kernel
// is still trying to reach the old address.
func hasConnectingPath(paths []path) bool {
	for _, p := range paths {
		if p.State == "connecting" {
			return true
		}
	}
	return false
}

// resolveExpectedPathCount returns the expected number of NVMe-oF paths for the
// given NQN. On first encounter it queries the API once to seed the cache so the
// monitor works correctly even if started while a volume is already degraded.
// Subsequent calls use the in-memory cache, which only grows upward.
func resolveExpectedPathCount(nqn, clusterID, lvolID string, currentActive int) int {
	maxSeenMu.Lock()
	cached, exists := maxSeenPathsMap[nqn]
	if currentActive > cached {
		cached = currentActive
		maxSeenPathsMap[nqn] = cached
	}
	maxSeenMu.Unlock()

	if exists {
		return cached
	}

	sbcClient, err := NewsimplyBlockClient(context.Background(), clusterID, "")
	if err != nil {
		klog.Warningf("resolveExpectedPathCount: client error for NQN %s: %v", nqn, err)
		return cached
	}
	conns, err := fetchLvolConnection(context.Background(), sbcClient, lvolID, "")
	if err != nil {
		klog.Warningf("resolveExpectedPathCount: fetch error for NQN %s: %v", nqn, err)
		return cached
	}

	maxSeenMu.Lock()
	if len(conns) > maxSeenPathsMap[nqn] {
		maxSeenPathsMap[nqn] = len(conns)
		cached = len(conns)
	}
	maxSeenMu.Unlock()

	return cached
}

func recoverPathsWithANA(clusterID, lvolID, devicePath string, activePaths []path) error {
	sbcClient, err := NewsimplyBlockClient(context.Background(), clusterID, "")
	if err != nil {
		return fmt.Errorf("failed to create SimplyBlock client: %w", err)
	}

	nodeInfo, err := fetchNodeInfo(context.Background(), sbcClient, lvolID)
	if err != nil {
		return fmt.Errorf("failed to fetch node info for lvol %s: %w", lvolID, err)
	}

	expectedConns, err := fetchLvolConnection(context.Background(), sbcClient, lvolID, "")
	if err != nil {
		return fmt.Errorf("failed to fetch connections for lvol %s: %w", lvolID, err)
	}
	if len(expectedConns) == 0 {
		return fmt.Errorf("API returned no connections for lvol %s", lvolID)
	}

	nqn := expectedConns[0].Nqn
	maxSeenMu.Lock()
	if len(expectedConns) > maxSeenPathsMap[nqn] {
		maxSeenPathsMap[nqn] = len(expectedConns)
	}
	maxSeenMu.Unlock()

	ctrlLossTmo := DefaultCtrlLossTmo

	optConn := expectedConns[0]
	nonOptConns := expectedConns[1:]

	activeOpt := filterByANA(activePaths, "optimized")

	var activeNonOpt []path
	for _, p := range activePaths {
		if parseAddress(p.Address) != optConn.IP {
			activeNonOpt = append(activeNonOpt, p)
		}
	}

	reconcileOptimizedPath(sbcClient, nodeInfo, devicePath, optConn, activeOpt, ctrlLossTmo)
	reconcileNonOptimizedPaths(sbcClient, nodeInfo, devicePath, nonOptConns, activeNonOpt, ctrlLossTmo)

	return nil
}

func reconcileOptimizedPath(
	sbcClient *ClusterClient,
	nodeInfo *NodeInfo,
	devicePath string,
	conn *LvolConnectResp,
	active []path,
	ctrlLossTmo int,
) {
	if len(active) == 0 {
		if !isNodeOnline(context.Background(), sbcClient, nodeInfo.NodeID, conn.IP, conn.Port) {
			klog.Infof("reconcileOptimizedPath: primary node %s not yet online, skipping", nodeInfo.NodeID)
			return
		}
		klog.Infof("reconcileOptimizedPath: connecting missing optimized path ip=%s", conn.IP)
		if err := connectViaNVMe(context.Background(), conn, ctrlLossTmo, 1); err != nil {
			klog.Errorf("reconcileOptimizedPath: connect to %s failed: %v", conn.IP, err)
		}
		return
	}

	activeIP := parseAddress(active[0].Address)
	if activeIP == conn.IP {
		return
	}

	if !isNodeOnline(context.Background(), sbcClient, nodeInfo.NodeID, conn.IP, conn.Port) {
		klog.Infof("reconcileOptimizedPath: primary node %s not yet online, skipping IP change reconnect", nodeInfo.NodeID)
		return
	}
	if err := connectViaNVMe(context.Background(), conn, ctrlLossTmo, 1); err != nil {
		klog.Errorf("reconcileOptimizedPath: connect to new IP %s failed: %v", conn.IP, err)
	}
}

// reconcileNonOptimizedPaths handles connections[1..N] (secondary nodes).
// Works for both 2-path (1 secondary) and 3-path (2 secondaries).
func reconcileNonOptimizedPaths(
	sbcClient *ClusterClient,
	nodeInfo *NodeInfo,
	devicePath string,
	conns []*LvolConnectResp,
	active []path,
	ctrlLossTmo int,
) {
	if len(conns) == 0 {
		return
	}

	activeIPMap := make(map[string]path)
	for _, p := range active {
		if ip := parseAddress(p.Address); ip != "" {
			activeIPMap[ip] = p
		}
	}

	// Build expected IP set.
	expectedIPSet := make(map[string]bool)
	for _, conn := range conns {
		expectedIPSet[conn.IP] = true
	}

	// Step 1: disconnect stale paths (IP no longer expected → node IP changed).
	for ip, _ := range activeIPMap {
		if !expectedIPSet[ip] {
			delete(activeIPMap, ip)
		}
	}

	onlineSecondaries := 0
	totalSecondaries := 0
	for _, nodeID := range nodeInfo.Nodes {
		if nodeID == nodeInfo.NodeID {
			continue // skip primary
		}
		totalSecondaries++
		if isNodeOnline(context.Background(), sbcClient, nodeID, "", 0) {
			onlineSecondaries++
		}
	}
	if totalSecondaries > 0 && onlineSecondaries == 0 {
		klog.Infof("reconcileNonOptimizedPaths: all %d secondary node(s) offline, skipping", totalSecondaries)
		return
	}

	if len(conns) > 0 && !isAnyConnReachable(context.Background(), conns) {
		klog.Infof("reconcileNonOptimizedPaths: no secondary NVMe-oF endpoints TCP-reachable, skipping")
		return
	}

	for _, conn := range conns {
		if _, exists := activeIPMap[conn.IP]; exists {
			continue
		}
		if !isTCPReachable(context.Background(), conn.IP, conn.Port) {
			klog.Infof("reconcileNonOptimizedPaths: %s:%d not TCP-reachable, skipping", conn.IP, conn.Port)
			continue
		}
		klog.Infof("reconcileNonOptimizedPaths: connecting missing path ip=%s", conn.IP)
		if err := connectViaNVMe(context.Background(), conn, ctrlLossTmo, 1); err != nil {
			klog.Errorf("reconcileNonOptimizedPaths: connect to %s failed: %v", conn.IP, err)
		}
	}
}

// filterByANA returns the subset of paths whose ANAState matches anaState.
func filterByANA(paths []path, anaState string) []path {
	var result []path
	for _, p := range paths {
		if p.ANAState == anaState {
			result = append(result, p)
		}
	}
	return result
}
