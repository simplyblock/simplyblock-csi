package util

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog"
)

var (
	// ErrClusterRegistryUnavailable is returned when the Simplyblock cluster configuration CRD
	// cannot be reached or parsed.
	ErrClusterRegistryUnavailable = errors.New("simplyblock cluster registry unavailable")
	// ErrClusterConfigNotFound indicates that a given cluster ID could not be resolved.
	ErrClusterConfigNotFound = errors.New("simplyblock cluster configuration not found")
	// ErrZoneNotMapped is returned when no cluster is mapped to a requested zone.
	ErrZoneNotMapped = errors.New("zone not mapped to simplyblock cluster")
)

const (
	clusterConfigNameEnv     = "SPDKCSI_CLUSTER_CONFIG_NAME"
	clusterConfigRefreshEnv  = "SPDKCSI_CLUSTER_CONFIG_REFRESH_INTERVAL"
	defaultClusterConfigName = "simplyblock-clusters"
	defaultRefreshInterval   = time.Minute
	clusterConfigGroup       = "csi.simplyblock.io"
	clusterConfigVersion     = "v1alpha1"
	clusterConfigResource    = "simplyblockclusterconfigs"
)

var clusterConfigGVR = schema.GroupVersionResource{
	Group:    clusterConfigGroup,
	Version:  clusterConfigVersion,
	Resource: clusterConfigResource,
}

type registryEntry struct {
	cluster  *ClusterConfig
	zone     string
	segments map[string]string
}

func (r *registryEntry) topologySegments() map[string]string {
	return copyStringMap(r.segments)
}

type clusterRegistry struct {
	clustersByID map[string]*ClusterConfig
	zones        map[string]*registryEntry
}

type clusterRegistryCache struct {
	sync.RWMutex
	registry        *clusterRegistry
	lastLoaded      time.Time
	refreshInterval time.Duration
}

var globalClusterRegistryCache clusterRegistryCache

// LookupClusterConfigByID returns the Simplyblock cluster configuration for the
// supplied cluster ID using the CRD-backed registry.
func LookupClusterConfigByID(clusterID string) (*ClusterConfig, error) {
	clusterID = strings.TrimSpace(clusterID)
	if clusterID == "" {
		return nil, ErrClusterConfigNotFound
	}

	registry, err := getClusterRegistry(context.Background())
	if err != nil {
		return nil, err
	}

	cluster, ok := registry.clustersByID[clusterID]
	if !ok {
		return nil, ErrClusterConfigNotFound
	}

	return cloneClusterConfig(cluster), nil
}

// LookupClusterByZone resolves the Simplyblock cluster responsible for the
// provided zone label.
func LookupClusterByZone(zone string) (*ClusterConfig, map[string]string, error) {
	zone = strings.TrimSpace(zone)
	if zone == "" {
		return nil, nil, ErrZoneNotMapped
	}

	registry, err := getClusterRegistry(context.Background())
	if err != nil {
		return nil, nil, err
	}

	entry, ok := registry.zones[zone]
	if !ok {
		return nil, nil, ErrZoneNotMapped
	}

	return cloneClusterConfig(entry.cluster), entry.topologySegments(), nil
}

// ListClusterIDsFromRegistry returns the list of cluster IDs discovered via the
// Simplyblock cluster configuration CRD.
func ListClusterIDsFromRegistry() ([]string, error) {
	registry, err := getClusterRegistry(context.Background())
	if err != nil {
		return nil, err
	}

	ids := make([]string, 0, len(registry.clustersByID))
	for id := range registry.clustersByID {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids, nil
}

// RefreshClusterRegistry forces the next lookup to reload the CRD contents.
func RefreshClusterRegistry(ctx context.Context) error {
	cache := &globalClusterRegistryCache
	cache.Lock()
	cache.registry = nil
	cache.lastLoaded = time.Time{}
	cache.Unlock()
	_, err := getClusterRegistry(ctx)
	return err
}

func getClusterRegistry(ctx context.Context) (*clusterRegistry, error) {
	cache := &globalClusterRegistryCache
	cache.RLock()
	reg := cache.registry
	valid := reg != nil && time.Since(cache.lastLoaded) < cache.refreshInterval()
	cache.RUnlock()
	if valid {
		return reg, nil
	}

	cache.Lock()
	defer cache.Unlock()
	if cache.registry != nil && time.Since(cache.lastLoaded) < cache.refreshInterval() {
		return cache.registry, nil
	}

	registry, err := loadClusterRegistry(ctx)
	if err != nil {
		return nil, err
	}

	cache.registry = registry
	cache.lastLoaded = time.Now()
	return registry, nil
}

func (c *clusterRegistryCache) refreshInterval() time.Duration {
	if c.refreshInterval != 0 {
		return c.refreshInterval
	}

	if val := strings.TrimSpace(os.Getenv(clusterConfigRefreshEnv)); val != "" {
		dur, err := time.ParseDuration(val)
		if err != nil || dur <= 0 {
			klog.Warningf("invalid %s value %q: %v", clusterConfigRefreshEnv, val, err)
		} else {
			c.refreshInterval = dur
			return c.refreshInterval
		}
	}

	c.refreshInterval = defaultRefreshInterval
	return c.refreshInterval
}

func loadClusterRegistry(ctx context.Context) (*clusterRegistry, error) {
	config, err := loadRESTConfig()
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrClusterRegistryUnavailable, err)
	}

	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to create dynamic client: %v", ErrClusterRegistryUnavailable, err)
	}

	name := FromEnv(clusterConfigNameEnv, defaultClusterConfigName)
	obj, err := dynamicClient.Resource(clusterConfigGVR).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("%w: failed to fetch SimplyBlockClusterConfig %s: %v", ErrClusterRegistryUnavailable, name, err)
	}

	clustersSlice, found, err := unstructured.NestedSlice(obj.Object, "spec", "clusters")
	if err != nil {
		return nil, fmt.Errorf("%w: invalid spec.clusters: %v", ErrClusterRegistryUnavailable, err)
	}
	if !found {
		return nil, fmt.Errorf("%w: spec.clusters not found", ErrClusterRegistryUnavailable)
	}

	registry := &clusterRegistry{
		clustersByID: make(map[string]*ClusterConfig),
		zones:        make(map[string]*registryEntry),
	}

	for idx, entry := range clustersSlice {
		clusterMap, ok := entry.(map[string]interface{})
		if !ok {
			klog.Warningf("ignoring clusters[%d]: expected object, got %T", idx, entry)
			continue
		}

		clusterID := firstNonEmpty(clusterMap,
			"clusterID", "clusterId", "cluster_id", "id",
		)
		if clusterID == "" {
			klog.Warningf("ignoring clusters[%d]: missing clusterID", idx)
			continue
		}

		endpoint := firstNonEmpty(clusterMap,
			"clusterEndpoint", "cluster_endpoint", "endpoint",
		)
		secret := firstNonEmpty(clusterMap,
			"clusterSecret", "cluster_secret", "secret",
		)

		topology := toStringMap(clusterMap["topology"])
		zones := toStringSlice(clusterMap["zones"])

		cluster := &ClusterConfig{
			ClusterID:       clusterID,
			ClusterEndpoint: endpoint,
			ClusterSecret:   secret,
			Topology:        topology,
			Zones:           zones,
		}

		registry.clustersByID[clusterID] = cluster

		if topoSlice, ok := clusterMap["topologies"].([]interface{}); ok {
			for topoIdx, topoVal := range topoSlice {
				segments := toStringMap(topoVal)
				zone := firstNonEmptyMap(segments, TopologyKeyZoneStable, TopologyKeyZoneBeta)
				if zone == "" {
					klog.Warningf("clusters[%d].topologies[%d] missing %s", idx, topoIdx, TopologyKeyZoneStable)
					continue
				}
				registerZone(registry, cluster, zone, segments)
			}
		}

		if len(cluster.Zones) > 0 {
			for _, zone := range cluster.Zones {
				registerZone(registry, cluster, zone, cluster.Topology)
			}
		} else if zone := firstNonEmptyMap(cluster.Topology, TopologyKeyZoneStable, TopologyKeyZoneBeta); zone != "" {
			registerZone(registry, cluster, zone, cluster.Topology)
		}
	}

	return registry, nil
}

func registerZone(registry *clusterRegistry, cluster *ClusterConfig, zone string, base map[string]string) {
	zone = strings.TrimSpace(zone)
	if zone == "" {
		return
	}

	if _, exists := registry.zones[zone]; exists {
		klog.Warningf("zone %s already mapped, skipping duplicate entry for cluster %s", zone, cluster.ClusterID)
		return
	}

	segments := copyStringMap(base)
	if segments == nil {
		segments = make(map[string]string)
	}
	segments[TopologyKeyZoneStable] = zone
	delete(segments, TopologyKeyZoneBeta)

	registry.zones[zone] = &registryEntry{
		cluster:  cluster,
		zone:     zone,
		segments: segments,
	}
}

func loadRESTConfig() (*rest.Config, error) {
	config, err := rest.InClusterConfig()
	if err == nil {
		return config, nil
	}

	kubeconfig := strings.TrimSpace(os.Getenv("KUBECONFIG"))
	if kubeconfig == "" {
		return nil, err
	}

	config, buildErr := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if buildErr != nil {
		return nil, buildErr
	}
	return config, nil
}

func copyStringMap(src map[string]string) map[string]string {
	if len(src) == 0 {
		return nil
	}

	out := make(map[string]string, len(src))
	for k, v := range src {
		out[k] = v
	}
	return out
}

func cloneClusterConfig(src *ClusterConfig) *ClusterConfig {
	if src == nil {
		return nil
	}

	clone := *src
	clone.Topology = copyStringMap(src.Topology)
	if len(src.Zones) > 0 {
		clone.Zones = append([]string(nil), src.Zones...)
	}
	return &clone
}

func firstNonEmpty(m map[string]interface{}, keys ...string) string {
	for _, key := range keys {
		if value, ok := m[key]; ok {
			str := strings.TrimSpace(fmt.Sprintf("%v", value))
			if str != "" {
				return str
			}
		}
	}
	return ""
}

func firstNonEmptyMap(m map[string]string, keys ...string) string {
	for _, key := range keys {
		if value, ok := m[key]; ok {
			value = strings.TrimSpace(value)
			if value != "" {
				return value
			}
		}
	}
	return ""
}

func toStringSlice(value interface{}) []string {
	arr, ok := value.([]interface{})
	if !ok {
		return nil
	}

	var result []string
	for _, item := range arr {
		str := strings.TrimSpace(fmt.Sprintf("%v", item))
		if str != "" {
			result = append(result, str)
		}
	}
	return result
}

func toStringMap(value interface{}) map[string]string {
	if value == nil {
		return nil
	}

	data, err := json.Marshal(value)
	if err != nil {
		klog.Warningf("failed to marshal topology data: %v", err)
		return nil
	}

	var result map[string]string
	if err := json.Unmarshal(data, &result); err != nil {
		klog.Warningf("failed to convert topology data: %v", err)
		return nil
	}

	for key, val := range result {
		result[key] = strings.TrimSpace(val)
	}
	return result
}
