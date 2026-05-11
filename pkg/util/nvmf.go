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
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"k8s.io/klog"
)

const (
	envTLSConnect = "SB_TLS_CONNECT"
	envTLSCAFile  = "SB_TLS_CERTIFICATE_AUTHORITY"
	envTLSCert    = "SB_TLS_CERTIFICATE"
	envTLSKey     = "SB_TLS_KEY"

	defaultTLSCAFile = "/etc/simplyblock/tls/ca.crt"
	defaultTLSCert   = "/etc/simplyblock/tls/tls.crt"
	defaultTLSKey    = "/etc/simplyblock/tls/tls.key"

	namespaceFile = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
)

type tlsMode int

const (
	tlsDisabled tlsMode = iota
	tlsAnonymous
	tlsAuthenticated
)

func parseTLSMode(s string) (tlsMode, error) {
	switch s {
	case "", "disabled":
		return tlsDisabled, nil
	case "anonymous":
		return tlsAnonymous, nil
	case "authenticated":
		return tlsAuthenticated, nil
	default:
		return tlsDisabled, fmt.Errorf("invalid %s value %q (want disabled, anonymous, or authenticated)", envTLSConnect, s)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// tlsServerName returns the FQDN service name that matches the TLS certificate
// SANs (e.g. "simplyblock-webappapi.simplyblock.svc") derived from the URL host
// and the pod's own namespace. Falls back to the bare hostname on any error.
func tlsServerName(clusterIP string) string {
	// strip scheme and port to get just the hostname
	host := clusterIP
	host = strings.TrimPrefix(host, "https://")
	host = strings.TrimPrefix(host, "http://")
	if i := strings.LastIndex(host, ":"); i != -1 {
		host = host[:i]
	}
	// if already a FQDN (contains a dot), use as-is
	if strings.Contains(host, ".") {
		return host
	}
	ns, err := os.ReadFile(namespaceFile)
	if err != nil {
		return host
	}
	return fmt.Sprintf("%s.%s.svc", host, strings.TrimSpace(string(ns)))
}

type NodeNVMf struct {
	Client *RPCClient
}

// NewNVMf creates a new NVMf client. The HTTP transport is selected by the
// SB_TLS_CONNECT environment variable: "disabled" (or unset) uses plain HTTP;
// "anonymous" uses HTTPS validated against SB_TLS_CERTIFICATE_AUTHORITY;
// "authenticated" adds a client cert/key from SB_TLS_CERTIFICATE / SB_TLS_KEY.
func NewNVMf(clusterID, clusterIP, clusterSecret string) (*NodeNVMf, error) {
	mode, err := parseTLSMode(os.Getenv(envTLSConnect))
	if err != nil {
		return nil, err
	}

	transport := http.DefaultTransport
	if mode != tlsDisabled {
		caFile := envOr(envTLSCAFile, defaultTLSCAFile)
		caData, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("read TLS CA %s: %w", caFile, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caData) {
			return nil, fmt.Errorf("no certificates parsed from TLS CA %s", caFile)
		}

		clusterIP = strings.Replace(clusterIP, "http://", "https://", 1)
		tlsCfg := &tls.Config{RootCAs: pool, ServerName: tlsServerName(clusterIP)}

		if mode == tlsAuthenticated {
			certFile := envOr(envTLSCert, defaultTLSCert)
			keyFile := envOr(envTLSKey, defaultTLSKey)
			cert, err := tls.LoadX509KeyPair(certFile, keyFile)
			if err != nil {
				return nil, fmt.Errorf("load TLS client keypair (%s, %s): %w", certFile, keyFile, err)
			}
			tlsCfg.Certificates = []tls.Certificate{cert}
		}

		transport = &http.Transport{TLSClientConfig: tlsCfg}
	}

	client := RPCClient{
		HTTPClient:    &http.Client{Timeout: cfgRPCTimeoutSeconds * time.Second, Transport: transport},
		ClusterID:     clusterID,
		ClusterIP:     clusterIP,
		ClusterSecret: clusterSecret,
	}
	return &NodeNVMf{Client: &client}, nil
}

func (node *NodeNVMf) Info() string {
	return node.Client.info()
}

func (node *NodeNVMf) LvStores() ([]LvStore, error) {
	return node.Client.lvStores()
}

// VolumeInfo returns a string:string map containing information necessary
// for CSI node(initiator) to connect to this target and identify the disk.
// hostNQN is passed to the sbcli API when the volume has allowed_hosts configured.
func (node *NodeNVMf) VolumeInfo(lvolID string, hostNQN string) (map[string]string, error) {
	return node.Client.getVolumeInfo(lvolID, hostNQN)
}

// CreateLVolData is the data structure for creating a logical volume
type CreateLVolData struct {
	LvolName     string `json:"name"`
	Size         string `json:"size"`
	LvsName      string `json:"pool"`
	Fabric       string `json:"fabric"`
	Compression  bool   `json:"comp"`
	Encryption   bool   `json:"crypto"`
	Replicate    bool   `json:"do_replicate"`
	MaxRWIOPS    string `json:"max_rw_iops"`
	MaxRWmBytes  string `json:"max_rw_mbytes"`
	MaxRmBytes   string `json:"max_r_mbytes"`
	MaxWmBytes   string `json:"max_w_mbytes"`
	MaxSize      string `json:"max_size"`
	MaxNamespace int    `json:"max_namespace_per_subsys"`
	DistNdcs     int    `json:"ndcs"`
	DistNpcs     int    `json:"npcs"`
	PriorClass   int    `json:"lvol_priority_class"`
	CryptoKey1   string `json:"crypto_key1"`
	CryptoKey2   string `json:"crypto_key2"`
	HostID       string `json:"host_id"`
	LvolID       string `json:"uid"`
	ModelID      string `json:"namespace"`
	PvcName      string `json:"pvc_name"`
}

// CreateVolume creates a logical volume and returns volume ID
func (node *NodeNVMf) CreateVolume(params *CreateLVolData) (string, error) {
	lvolID, err := node.Client.createVolume(params)
	if err != nil {
		return "", err
	}
	klog.V(5).Infof("volume created: %s", lvolID)
	return lvolID, nil
}

// GetVolume returns the volume id of the given volume name and lvstore name. return error if not found.
func (node *NodeNVMf) GetVolume(lvolName, poolName string) (string, error) {
	lvol, err := node.Client.getVolume(fmt.Sprintf("%s/%s", poolName, lvolName))
	if err != nil {
		return "", err
	}
	return lvol.UUID, err
}

// GetVolumeSize returns the size of the volume
func (node *NodeNVMf) GetVolumeSize(lvolID string) (string, error) {
	lvol, err := node.Client.getVolume(lvolID)
	if err != nil {
		return "", err
	}

	size := strconv.FormatInt(lvol.LvolSize, 10)
	return size, err
}

// ListVolumes returns a list of volumes
func (node *NodeNVMf) ListVolumes() ([]*BDev, error) {
	return node.Client.listVolumes()
}

// GetMasterLvols returns master lvols for the given pool UUID
func (node *NodeNVMf) GetMasterLvols(poolUUID string) ([]MasterLvol, error) {
	return node.Client.getMasterLvols(poolUUID)
}

// GetPoolUUIDByName returns the UUID of the pool with the given name
func (node *NodeNVMf) GetPoolUUIDByName(poolName string) (string, error) {
	return node.Client.getPoolUUIDByName(poolName)
}

// ResizeVolume resizes a volume
func (node *NodeNVMf) ResizeVolume(lvolID string, newSize int64) (bool, error) {
	return node.Client.resizeVolume(lvolID, newSize)
}

// ListSnapshots returns a list of snapshots
func (node *NodeNVMf) ListSnapshots() ([]*SnapshotResp, error) {
	return node.Client.listSnapshots()
}

// CloneSnapshot clones a snapshot to a new volume
func (node *NodeNVMf) CloneSnapshot(snapshotID, cloneName, newSize, pvcName string) (string, error) {
	lvolID, err := node.Client.cloneSnapshot(snapshotID, cloneName, newSize, pvcName)
	if err != nil {
		return "", err
	}
	klog.V(5).Infof("snapshot cloned: %s", lvolID)
	return lvolID, nil
}

// CloneVolume clones a volume to a new volume
func (node *NodeNVMf) CloneVolume(lvolID, cloneName, newSize, pvcName string) (string, error) {
	lvolID, err := node.Client.cloneVolume(lvolID, cloneName, newSize, pvcName)
	if err != nil {
		return "", err
	}
	klog.V(5).Infof("snapshot cloned: %s", lvolID)
	return lvolID, nil
}

// CreateSnapshot creates a snapshot of a volume
func (node *NodeNVMf) CreateSnapshot(lvolID, snapshotName string) (string, error) {
	snapshotID, err := node.Client.snapshot(lvolID, snapshotName)
	if err != nil {
		return "", err
	}
	snapshotID = fmt.Sprintf("%s:%s", node.Client.ClusterID, snapshotID)
	klog.V(5).Infof("snapshot created: %s", snapshotID)
	return snapshotID, nil
}

// DeleteVolume deletes a volume
func (node *NodeNVMf) DeleteVolume(lvolID string) error {
	err := node.Client.deleteVolume(lvolID)
	if err != nil {
		return err
	}
	klog.V(5).Infof("volume deleted: %s", lvolID)
	return nil
}

// DeleteSnapshot deletes a snapshot
func (node *NodeNVMf) DeleteSnapshot(snapshotID string) error {
	err := node.Client.deleteSnapshot(snapshotID)
	if err != nil {
		return err
	}
	klog.V(5).Infof("snapshot deleted: %s", snapshotID)
	return nil
}

// PublishVolume exports a volume through NVMf target
func (node *NodeNVMf) PublishVolume(lvolID string) error {
	_, err := node.Client.CallSBCLI("GET", "/lvol/"+lvolID, nil)
	if err != nil {
		return err
	}
	klog.V(5).Infof("volume published: %s", lvolID)
	return nil
}

// UnpublishVolume unexports a volume through NVMf target
func (node *NodeNVMf) UnpublishVolume(lvolID string) error {
	_, err := node.Client.CallSBCLI("GET", "/lvol/"+lvolID, nil)
	if err != nil {
		return err
	}

	klog.V(5).Infof("volume unpublished: %s", lvolID)
	return nil
}
