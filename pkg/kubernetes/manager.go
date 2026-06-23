// Package kubernetes provides a Manager that serves Kubernetes object reads
// from watch-backed, in-memory caches, transparently falling back to direct
// API reads whenever the cache is not (yet) available. Hot paths get cache
// performance without having to handle cache setup or sync failures themselves.
package kubernetes

import (
	"context"

	"k8s.io/client-go/informers"
	k8sclient "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog"
)

// csiDriverIndex is the name of the informer index that groups
// PersistentVolumes by their CSI driver.
const csiDriverIndex = "csiDriver"

// lvolIndex is the name of the informer index that maps PersistentVolumes by
// the lvol (logical volume) ID encoded in their CSI volume handle.
const lvolIndex = "lvol"

// Manager owns the shared informer caches for the resources the CSI driver
// reads on hot paths (PersistentVolumes, PersistentVolumeClaims). Every read
// goes through the cache when it is synced and falls back to a direct API read
// otherwise, so callers never have to deal with an unavailable cache.
//
// A nil *Manager is valid and means "no Kubernetes access at all" (e.g. no
// in-cluster client could be built); its read methods return empty results so
// callers can keep operating in a degraded mode.
type Manager struct {
	client      k8sclient.Interface
	factory     informers.SharedInformerFactory
	pvInformer  cache.SharedIndexInformer
	pvcInformer cache.SharedIndexInformer
}

// NewManager builds a Manager backed by the given client. Call Start to launch
// the caches. Returns nil when client is nil, since without a client there is
// neither a cache nor an API to fall back to.
func NewManager(client k8sclient.Interface) *Manager {
	if client == nil {
		return nil
	}

	// resync period 0: rely purely on watch deltas, never re-List periodically.
	factory := informers.NewSharedInformerFactory(client, 0)
	pvInformer := factory.Core().V1().PersistentVolumes().Informer()
	pvcInformer := factory.Core().V1().PersistentVolumeClaims().Informer()

	if err := pvInformer.AddIndexers(cache.Indexers{
		csiDriverIndex: indexPersistentVolumeByCSIDriver,
		lvolIndex:      indexPersistentVolumeByLvolID,
	}); err != nil {
		// Only fails on a duplicate index name or an already-started informer,
		// neither of which can happen here. Reads stay correct via the API
		// fallback even without the index, so log and continue.
		klog.Warningf("kubernetes cache manager: failed to register PersistentVolume "+
			"indexers, those reads will fall back to the API: %v", err)
	}

	return &Manager{
		client:      client,
		factory:     factory,
		pvInformer:  pvInformer,
		pvcInformer: pvcInformer,
	}
}

// Start launches the informers, which LIST-then-WATCH in the background and
// populate the caches automatically; there is no manual sync step. Reads check
// HasSynced per call and fall back to the API until the initial LIST completes,
// so Start need not block on sync. It is a no-op on a nil Manager and should be
// called once from a single owner. The informers run until ctx is cancelled.
func (m *Manager) Start(ctx context.Context) {
	if m == nil {
		return
	}
	m.factory.Start(ctx.Done())
}

// Client returns the underlying Kubernetes client, for reads of resources the
// Manager does not cache (e.g. Pods, StorageClasses). Returns nil on a nil
// Manager.
func (m *Manager) Client() k8sclient.Interface {
	if m == nil {
		return nil
	}
	return m.client
}
