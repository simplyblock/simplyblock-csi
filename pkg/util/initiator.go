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
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"k8s.io/klog"
)

// SpdkCsiInitiator defines interface for NVMeoF/iSCSI initiator
//   - Connect initiates target connection and returns local block device filename
//     e.g., /dev/disk/by-id/nvme-SPDK_Controller1_SPDK00000000000001
//   - Disconnect terminates target connection
//   - Caller(node service) should serialize calls to same initiator
//   - Implementation should be idempotent to duplicated requests
type SpdkCsiInitiator interface {
	Connect() (string, error)
	Disconnect() error
}

const DevDiskByID = "/dev/disk/by-id/*%s*"

func NewSpdkCsiInitiator(volumeContext map[string]string, spdkNode *NodeNVMf) (SpdkCsiInitiator, error) {
	targetType := strings.ToLower(volumeContext["targetType"])
	switch targetType {
	case "rdma", "tcp":
		var connections []connectionInfo
		err := json.Unmarshal([]byte(volumeContext["connections"]), &connections)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshall connections. Error: %v", err.Error())
		}
		return &initiatorNVMf{
			// see util/nvmf.go VolumeInfo()
			targetType:     volumeContext["targetType"],
			connections:    connections,
			nqn:            volumeContext["nqn"],
			reconnectDelay: volumeContext["reconnectDelay"],
			nrIoQueues:     volumeContext["nrIoQueues"],
			ctrlLossTmo:    volumeContext["ctrlLossTmo"],
			model:          volumeContext["model"],
			client:         *spdkNode.client,
		}, nil
	case "cache":
		return &initiatorCache{
			lvol:   volumeContext["uuid"],
			model:  volumeContext["model"],
			client: *spdkNode.client,
		}, nil
	default:
		return nil, fmt.Errorf("unknown initiator: %s", targetType)
	}
}

// NVMf initiator implementation
type initiatorNVMf struct {
	targetType     string
	connections    []connectionInfo
	nqn            string
	reconnectDelay string
	nrIoQueues     string
	ctrlLossTmo    string
	model          string
	client         RPCClient
}

type initiatorCache struct {
	lvol   string
	model  string
	client RPCClient
}

type cachingNodeList struct {
	Hostname string `json:"hostname"`
	UUID     string `json:"id"`
}

type LVolCachingNodeConnect struct {
	LvolID string `json:"lvol_id"`
}

type Subsystem struct {
	Name  string `json:"Name"`
	NQN   string `json:"NQN"`
	Paths []Path `json:"Paths"`
}

type Path struct {
	Name      string `json:"Name"`
	Transport string `json:"Transport"`
	Address   string `json:"Address"`
	State     string `json:"State"`
}

type SubsystemResponse struct {
	Subsystems []Subsystem `json:"Subsystems"`
}

func (cache *initiatorCache) Connect() (string, error) {
	// get the hostname
	hostname, err := os.Hostname()
	if err != nil {
		os.Exit(1)
	}
	hostname = strings.Split(hostname, ".")[0]
	klog.Info("hostname: ", hostname)

	out, err := cache.client.CallSBCLI("GET", "/cachingnode", nil)
	if err != nil {
		klog.Error(err)
		return "", err
	}

	data, err := json.Marshal(out)
	if err != nil {
		return "", err
	}
	var cnodes []*cachingNodeList
	err = json.Unmarshal(data, &cnodes)
	if err != nil {
		return "", err
	}

	klog.Info("found caching nodes: ", cnodes)

	isCachingNodeConnected := false
	for _, cnode := range cnodes {
		if hostname != cnode.Hostname {
			continue
		}

		var resp interface{}
		req := LVolCachingNodeConnect{
			LvolID: cache.lvol,
		}
		klog.Info("connecting caching node: ", cnode.Hostname, " with lvol: ", cache.lvol)
		resp, err = cache.client.CallSBCLI("PUT", "/cachingnode/connect/"+cnode.UUID, req)
		if err != nil {
			klog.Error("caching node connect error:", err)
			return "", err
		}
		klog.Info("caching node connect resp: ", resp)
		isCachingNodeConnected = true
	}

	if !isCachingNodeConnected {
		return "", errors.New("failed to find the caching node")
	}

	// get the caching node ID associated with the hostname
	// connect lvol and caching node

	deviceGlob := fmt.Sprintf(DevDiskByID, cache.model)
	devicePath, err := waitForDeviceReady(deviceGlob, 20)
	if err != nil {
		return "", err
	}
	return devicePath, nil
}

func (cache *initiatorCache) Disconnect() error {
	// get the hostname
	// get the caching node ID associated with the hostname
	// connect lvol and caching node

	hostname, err := os.Hostname()
	if err != nil {
		os.Exit(1)
	}
	hostname = strings.Split(hostname, ".")[0]
	klog.Info("hostname: ", hostname)

	out, err := cache.client.CallSBCLI("GET", "/cachingnode", nil)
	if err != nil {
		klog.Error(err)
		return err
	}

	data, err := json.Marshal(out)
	if err != nil {
		return err
	}
	var cnodes []*cachingNodeList
	err = json.Unmarshal(data, &cnodes)
	if err != nil {
		return err
	}
	klog.Info("found caching nodes: ", cnodes)

	isCachingNodeConnected := false
	for _, cnode := range cnodes {
		if hostname != cnode.Hostname {
			continue
		}
		klog.Info("disconnect caching node: ", cnode.Hostname, "with lvol: ", cache.lvol)
		req := LVolCachingNodeConnect{
			LvolID: cache.lvol,
		}
		resp, err := cache.client.CallSBCLI("PUT", "/cachingnode/disconnect/"+cnode.UUID, req)
		if err != nil {
			klog.Error("caching node disconnect error:", err)
			return err
		}
		klog.Info("caching node disconnect resp: ", resp)
		isCachingNodeConnected = true
	}

	if !isCachingNodeConnected {
		return errors.New("failed to find the caching node")
	}

	deviceGlob := fmt.Sprintf(DevDiskByID, cache.model)
	return waitForDeviceGone(deviceGlob)
}

func execWithTimeoutRetry(cmdLine []string, timeout, retry int) (err error) {
	for retry > 0 {
		err = execWithTimeout(cmdLine, timeout)
		if err == nil {
			return nil
		}
		retry--
	}
	return err
}

func (nvmf *initiatorNVMf) updateConnectionInfo() error {
	parts := strings.Split(nvmf.nqn, ":")
	if len(parts) < 4 || parts[2] != "lvol" {
		return fmt.Errorf("invalid NQN format, lvol_id not found: %s", nvmf.nqn)
	}
	lvolID := parts[3]

	resp, err := nvmf.client.CallSBCLI("GET", "/lvol/connect/"+lvolID, nil)
	if err != nil {
		klog.Errorf("failed to fetch connection details for lvol_id %s: %v", lvolID, err)
		return err
	}

	var result []*LvolConnectResp
	respBytes, err := json.Marshal(resp)
	if err != nil {
		return fmt.Errorf("failed to marshal response: %v", err)
	}

	if err := json.Unmarshal(respBytes, &result); err != nil {
		return fmt.Errorf("failed to unmarshal connection details: %v", err)
	}

	for i := range nvmf.connections {
		nvmf.connections[i].IP = result[i].IP
	}
	return nil
}

func (nvmf *initiatorNVMf) Connect() (string, error) {

	if err := nvmf.updateConnectionInfo(); err != nil {
		return "", fmt.Errorf("failed to update connection info: %v", err)
	}

	// nvme connect -t tcp -a 192.168.1.100 -s 4420 -n "nqn"
	klog.Info("connections", nvmf.connections)
	for i, conn := range nvmf.connections {
		cmdLine := []string{
			"nvme", "connect", "-t", strings.ToLower(nvmf.targetType),
			"-a", conn.IP, "-s", strconv.Itoa(conn.Port), "-n", nvmf.nqn, "-l", nvmf.ctrlLossTmo,
			"-c", nvmf.reconnectDelay, "-i", nvmf.nrIoQueues,
		}
		err := execWithTimeoutRetry(cmdLine, 40, len(nvmf.connections))
		if err != nil {
			// go on checking device status in case caused by duplicated request
			klog.Errorf("command %v failed: %s", cmdLine, err)

			// disconnect the primary connection if secondary connection fails
			if i == 1 {
				klog.Warning("Secondary connection failed, disconnecting primary...")

				disconnectCmd := []string{"nvme", "disconnect", "-n", nvmf.nqn}
				disconnectErr := execWithTimeoutRetry(disconnectCmd, 40, 1)
				if disconnectErr != nil {
					klog.Errorf("Failed to disconnect primary: %v", disconnectErr)
				} else {
					klog.Infof("Primary connection disconnected due to secondary failure")
				}
			}

			return "", err
		}
	}

	deviceGlob := fmt.Sprintf(DevDiskByID, nvmf.model)
	devicePath, err := waitForDeviceReady(deviceGlob, 20)
	if err != nil {
		return "", err
	}
	return devicePath, nil
}

func (nvmf *initiatorNVMf) Disconnect() error {
	// nvme disconnect -n "nqn"
	cmdLine := []string{"nvme", "disconnect", "-n", nvmf.nqn}
	err := execWithTimeout(cmdLine, 40)
	if err != nil {
		// go on checking device status in case caused by duplicate request
		klog.Errorf("command %v failed: %s", cmdLine, err)
	}

	deviceGlob := fmt.Sprintf(DevDiskByID, nvmf.model)
	return waitForDeviceGone(deviceGlob)
}

// when timeout is set as 0, try to find the device file immediately
// otherwise, wait for device file comes up or timeout
func waitForDeviceReady(deviceGlob string, seconds int) (string, error) {
	for i := 0; i <= seconds; i++ {
		matches, err := filepath.Glob(deviceGlob)
		if err != nil {
			return "", err
		}
		// two symbol links under /dev/disk/by-id/ to same device
		if len(matches) >= 1 {
			return matches[0], nil
		}
		time.Sleep(time.Second)
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
func execWithTimeout(cmdLine []string, timeout int) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
	defer cancel()

	klog.Infof("running command: %v", cmdLine)
	//nolint:gosec // execWithTimeout assumes valid cmd arguments
	cmd := exec.CommandContext(ctx, cmdLine[0], cmdLine[1:]...)
	output, err := cmd.CombinedOutput()

	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return errors.New("timed out")
	}
	if output != nil {
		klog.Infof("command returned: %s", output)
	}
	return err
}

func getLvolIDFromNQN(nqn string) string {
	parts := strings.Split(nqn, ":lvol:")
	if len(parts) > 1 {
		return parts[1]
	}
	return ""
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

func getPathANAState(pathName string) (string, error) {
	cmd := exec.Command("nvme", "ana-log", fmt.Sprintf("/dev/%s", pathName), "-o", "json")
	output, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("failed to execute nvme ana-log for %s: %v", pathName, err)
	}

	var anaData struct {
		Descriptors []struct {
			State string `json:"state"`
		} `json:"ANA DESC LIST "`
	}

	if err := json.Unmarshal(output, &anaData); err != nil {
		return "", fmt.Errorf("failed to parse ANA log output for %s: %v", pathName, err)
	}

	if len(anaData.Descriptors) == 0 {
		return "", fmt.Errorf("no ANA state found for %s", pathName)
	}

	return anaData.Descriptors[0].State, nil
}

func reconnectSubsystems(spdkNode *NodeNVMf) error {
	cmd := exec.Command("nvme", "list-subsys", "-o", "json")
	output, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("failed to execute nvme list-subsys: %v", err)
	}

	var subsystems []SubsystemResponse
	if err := json.Unmarshal(output, &subsystems); err != nil {
		return fmt.Errorf("failed to unmarshal nvme list-subsys output: %v", err)
	}

	for _, host := range subsystems {
		for _, subsystem := range host.Subsystems {
			lvolID := getLvolIDFromNQN(subsystem.NQN)
			if lvolID == "" {
				continue
			}

			for _, path := range subsystem.Paths {
				anaState, err := getPathANAState(path.Name)
				if err != nil {
					klog.Errorf("failed to get ANA state for path %s: %v", path.Name, err)
					continue
				}

				if path.State == "connecting" && anaState == "optimized" {
					currentIP := parseAddress(path.Address)

					// Call the API for connection details
					resp, err := spdkNode.client.CallSBCLI("GET", "/lvol/connect/"+lvolID, nil)
					if err != nil {
						klog.Errorf("failed to fetch connection details for lvol_id %s: %v\n", lvolID, err)
						continue
					}

					var lvolResp []*LvolConnectResp

					respBytes, err := json.Marshal(resp)
					if err != nil {
						return fmt.Errorf("failed to marshal response: %v", err)
					}

					if err := json.Unmarshal(respBytes, &lvolResp); err != nil {
						return fmt.Errorf("failed to unmarshal connection details: %v", err)
					}

					if len(lvolResp) == 0 && lvolResp[0] == nil {
						klog.Errorf("unexpected response format or empty results")
						continue
					}

					updatedIP := lvolResp[0].IP
					nqn := lvolResp[0].Nqn
					port := lvolResp[0].Port
					ctrlLossTmo := lvolResp[0].CtrlLossTmo
					reconnectDelay := lvolResp[0].ReconnectDelay
					nrIoQueues := lvolResp[0].NrIoQueues

					if currentIP != updatedIP {
						klog.Infof("Updating connection for lvol_id %s: disconnecting %s and connecting to %s\n", lvolID, currentIP, updatedIP)

						// Disconnect the old path
						disconnectCmd := []string{
							"nvme", "disconnect", "-d", path.Name,
						}
						err = execWithTimeoutRetry(disconnectCmd, 40, 1)
						if err != nil {
							klog.Errorf("command %s failed: %v", disconnectCmd, err)
							return err
						}

						// Connect the new path
						cmdLine := []string{
							"nvme", "connect", "-t", "tcp",
							"-a", updatedIP, "-s", strconv.Itoa(port), "-n", nqn, "-l", strconv.Itoa(ctrlLossTmo),
							"-c", strconv.Itoa(reconnectDelay), "-i", strconv.Itoa(nrIoQueues),
						}
						err := execWithTimeoutRetry(cmdLine, 40, 1)
						if err != nil {
							klog.Errorf("command %s failed: %v", cmdLine, err)
							return err
						}

					}
				}
			}
		}
	}

	return nil
}

func MonitorConnection(spdkNode *NodeNVMf) {

	for {
		if spdkNode.client == nil {
			klog.Errorf("RPC client is not initialized")
			continue
		}

		if err := reconnectSubsystems(spdkNode); err != nil {
			klog.Errorf("Error: %v\n", err)
			continue
		}

		time.Sleep(3 * time.Second)
	}
}
