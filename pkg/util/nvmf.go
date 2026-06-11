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
	"crypto/tls"
	"crypto/x509"
	"errors"
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

type ClusterClient struct {
	API    *APIClient
	poolID string // pool scope for this client; empty means cluster-level only
}

func (c *ClusterClient) ClusterID() string { return c.API.ClusterID }
func (c *ClusterClient) PoolID() string    { return c.poolID }

// poolForVolume returns the pool ID for lvolID. If this client is already
// scoped to a pool, that pool ID is returned immediately. Otherwise all pools
// are scanned to locate the volume.
func (c *ClusterClient) poolForVolume(ctx context.Context, lvolID string) (string, error) {
	if c.poolID != "" {
		return c.poolID, nil
	}
	return c.API.findPoolForVolume(ctx, lvolID)
}

// NewConnection builds a shared HTTP connection to a webappapi endpoint.
// TLS is configured from environment variables:
//   - SB_TLS_CONNECT: "disabled" (default), "anonymous", or "authenticated"
//   - SB_TLS_CERTIFICATE_AUTHORITY: path to CA bundle
//   - SB_TLS_CERTIFICATE / SB_TLS_KEY: client cert/key for authenticated mode
//
// The returned Connection may be shared across multiple APIClients that
// all reach the same webappapi service.
func NewConnection(endpoint string) (*Connection, error) {
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

		endpoint = strings.Replace(endpoint, "http://", "https://", 1)
		tlsCfg := &tls.Config{RootCAs: pool, ServerName: tlsServerName(endpoint)}

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

	return &Connection{
		Endpoint: endpoint,
		HTTP:     &http.Client{Timeout: cfgRPCTimeoutSeconds * time.Second, Transport: transport},
	}, nil
}

// NewClusterClient creates a cluster-scoped API client.
// It builds a Connection to the webappapi endpoint and binds it to the
// given cluster credentials. For clusters that share an endpoint, prefer
// building one Connection via NewConnection and constructing APIClient directly.
func NewClusterClient(clusterID, endpoint, clusterSecret string) (*ClusterClient, error) {
	conn, err := NewConnection(endpoint)
	if err != nil {
		return nil, err
	}
	client := APIClient{
		ClusterID:     clusterID,
		ClusterSecret: clusterSecret,
		conn:          conn,
	}
	return &ClusterClient{API: &client}, nil
}

func (c *ClusterClient) Info() string {
	return c.API.info()
}

func (c *ClusterClient) ListStoragePools(ctx context.Context) ([]StoragePool, error) {
	return c.API.listStoragePools(ctx)
}

// VolumeInfo returns a string:string map containing information necessary
// for CSI node(initiator) to connect to this target and identify the disk.
// hostNQN is passed to the sbcli API when the volume has allowed_hosts configured.
func (c *ClusterClient) VolumeInfo(ctx context.Context, lvolID string, hostNQN string) (map[string]string, error) {
	return c.API.getVolumeInfo(ctx, c.poolID, lvolID, hostNQN)
}

// CreateLVolData is the data structure for creating a logical volume
type CreateLVolData struct {
	LvolName     string `json:"name"`
	Size         string `json:"size"`
	LvsName      string `json:"pool"`
	Fabric       string `json:"fabric"`
	Compression  bool   `json:"comp"`
	Encryption   bool   `json:"encrypt"`
	Replicate    bool   `json:"do_replicate"`
	MaxRWIOPS    string `json:"max_rw_iops"`
	MaxRWmBytes  string `json:"max_rw_mbytes"`
	MaxRmBytes   string `json:"max_r_mbytes"`
	MaxWmBytes   string `json:"max_w_mbytes"`
	MaxSize      string `json:"max_size"`
	MaxNamespace int    `json:"max_namespace_per_subsys"`
	DistNdcs     int    `json:"ndcs"`
	DistNpcs     int    `json:"npcs"`
	PriorClass   int    `json:"priority_class"`
	HostID       string `json:"host_id"`
	LvolID       string `json:"uid"`
	Namespaced   bool   `json:"namespaced"`
	PvcName      string `json:"pvc_name"`
}

// CreateVolume creates a logical volume and returns volume ID
func (c *ClusterClient) CreateVolume(ctx context.Context, params *CreateLVolData) (string, error) {
	lvolID, err := c.API.createVolume(ctx, c.poolID, params)
	if err != nil {
		return "", err
	}
	klog.V(5).Infof("volume created: %s", lvolID)
	return lvolID, nil
}

// GetVolumeSize returns the size of the volume
func (c *ClusterClient) GetVolumeSize(ctx context.Context, lvolID string) (string, error) {
	lvol, err := c.API.getVolumeByUUID(ctx, c.poolID, lvolID)
	if err != nil {
		return "", err
	}

	size := strconv.FormatInt(lvol.LvolSize, 10)
	return size, err
}

// ListVolumes returns a list of volumes
func (c *ClusterClient) ListVolumes(ctx context.Context) ([]*LvolResp, error) {
	return c.API.listVolumes(ctx, c.poolID)
}

// GetMasterLvols returns master lvols for the given pool UUID
func (c *ClusterClient) GetMasterLvols(ctx context.Context, poolUUID string) ([]MasterLvol, error) {
	return c.API.getMasterLvols(ctx, poolUUID)
}

// GetPoolUUIDByName returns the UUID of the pool with the given name
func (c *ClusterClient) GetPoolUUIDByName(ctx context.Context, poolName string) (string, error) {
	return c.API.getPoolUUIDByName(ctx, poolName)
}

// ResizeVolume resizes a volume
func (c *ClusterClient) ResizeVolume(ctx context.Context, lvolID string, newSize int64) error {
	return c.API.resizeVolume(ctx, c.poolID, lvolID, newSize)
}

// ListSnapshots returns a list of snapshots. When PoolID is not set, iterates all pools.
func (c *ClusterClient) ListSnapshots(ctx context.Context) ([]*SnapshotResp, error) {
	if c.poolID == "" {
		return c.API.listAllSnapshots(ctx)
	}
	return c.API.listSnapshots(ctx, c.poolID)
}

// CloneSnapshot clones a snapshot to a new volume
func (c *ClusterClient) CloneSnapshot(ctx context.Context, snapshotID, cloneName, newSize, pvcName string) (string, error) {
	lvolID, err := c.API.cloneSnapshot(ctx, c.poolID, snapshotID, cloneName, newSize, pvcName)
	if err != nil {
		return "", err
	}
	klog.V(5).Infof("snapshot cloned: %s", lvolID)
	return lvolID, nil
}

// CloneVolume clones a volume to a new volume
func (c *ClusterClient) CloneVolume(ctx context.Context, lvolID, cloneName, newSize, pvcName string) (string, error) {
	lvolID, err := c.API.cloneVolume(ctx, c.poolID, lvolID, cloneName, newSize, pvcName)
	if err != nil {
		return "", err
	}
	klog.V(5).Infof("snapshot cloned: %s", lvolID)
	return lvolID, nil
}

// CreateSnapshot creates a snapshot of a volume.
// Returns a 3-part CSI snapshot ID: {clusterID}:{poolID}:{snapshotUUID}
func (c *ClusterClient) CreateSnapshot(ctx context.Context, lvolID, snapshotName string) (string, error) {
	snapshotID, err := c.API.snapshot(ctx, c.poolID, lvolID, snapshotName)
	if err != nil {
		return "", err
	}
	csiID := fmt.Sprintf("%s:%s:%s", c.API.ClusterID, c.poolID, snapshotID)
	klog.V(5).Infof("snapshot created: %s", csiID)
	return csiID, nil
}

// DeleteVolume deletes a volume
func (c *ClusterClient) DeleteVolume(ctx context.Context, lvolID string) error {
	err := c.API.deleteVolume(ctx, c.poolID, lvolID)
	if err != nil {
		return err
	}
	klog.V(5).Infof("volume deleted: %s", lvolID)
	return nil
}

// DeleteSnapshot deletes a snapshot
func (c *ClusterClient) DeleteSnapshot(ctx context.Context, snapshotID string) error {
	err := c.API.deleteSnapshot(ctx, c.poolID, snapshotID)
	if err != nil && !errors.Is(err, ErrJSONNoSuchDevice) {
		return err
	}
	klog.V(5).Infof("snapshot deleted: %s", snapshotID)
	return nil
}

// PublishVolume exports a volume through NVMf target
func (c *ClusterClient) PublishVolume(ctx context.Context, lvolID string) error {
	_, err := c.API.do(ctx, http.MethodGet, c.API.v2volume(c.poolID, lvolID), nil)
	if err != nil {
		return err
	}
	klog.V(5).Infof("volume published: %s", lvolID)
	return nil
}

// UnpublishVolume unexports a volume through NVMf target
func (c *ClusterClient) UnpublishVolume(ctx context.Context, lvolID string) error {
	_, err := c.API.do(ctx, http.MethodGet, c.API.v2volume(c.poolID, lvolID), nil)
	if err != nil {
		if strings.Contains(err.Error(), "404") {
			return ErrVolumeUnpublished
		}
		return err
	}
	klog.V(5).Infof("volume unpublished: %s", lvolID)
	return nil
}
