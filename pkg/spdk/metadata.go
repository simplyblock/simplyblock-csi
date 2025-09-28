package spdk

import (
	"context"
	"fmt"
	"sync"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog"
)

const (
	volumeContextModeKey = "volumeMode"
	volumeModeBlock      = "block"
	volumeModeFilesystem = "filesystem"
)

type volumeMetadata struct {
	mode          string
	capacityBytes int64
}

type volumeMetadataStore struct {
	mu      sync.RWMutex
	volumes map[string]volumeMetadata
}

var globalVolumeMetadata = newVolumeMetadataStore()

func newVolumeMetadataStore() *volumeMetadataStore {
	return &volumeMetadataStore{
		volumes: make(map[string]volumeMetadata),
	}
}

func (s *volumeMetadataStore) Set(volumeID string, meta volumeMetadata) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.volumes[volumeID] = meta
}

func (s *volumeMetadataStore) UpdateCapacity(volumeID string, capacityBytes int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	meta, ok := s.volumes[volumeID]
	if !ok {
		meta = volumeMetadata{}
	}
	meta.capacityBytes = capacityBytes
	s.volumes[volumeID] = meta
}

func (s *volumeMetadataStore) UpdateMode(volumeID, mode string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	meta, ok := s.volumes[volumeID]
	if !ok {
		meta = volumeMetadata{}
	}
	meta.mode = mode
	s.volumes[volumeID] = meta
}

func (s *volumeMetadataStore) Delete(volumeID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.volumes, volumeID)
}

func (s *volumeMetadataStore) TryGet(volumeID string) (volumeMetadata, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	meta, ok := s.volumes[volumeID]
	return meta, ok
}

func (s *volumeMetadataStore) Get(ctx context.Context, volumeID string) (volumeMetadata, error) {
	s.mu.RLock()
	meta, ok := s.volumes[volumeID]
	s.mu.RUnlock()
	if ok && meta.mode != "" {
		return meta, nil
	}

	meta, err := hydrateVolumeMetadataFromCluster(ctx, volumeID)
	if err != nil {
		return volumeMetadata{}, err
	}

	if meta.mode == "" && meta.capacityBytes == 0 {
		return meta, nil
	}

	s.Set(volumeID, meta)
	return meta, nil
}

func hydrateVolumeMetadataFromCluster(ctx context.Context, volumeID string) (volumeMetadata, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return volumeMetadata{}, fmt.Errorf("could not get in-cluster config: %w", err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return volumeMetadata{}, fmt.Errorf("could not create clientset: %w", err)
	}

	pvs, err := clientset.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return volumeMetadata{}, fmt.Errorf("could not list PVs: %w", err)
	}

	for idx := range pvs.Items {
		pv := &pvs.Items[idx]
		if pv.Spec.CSI == nil {
			continue
		}
		if pv.Spec.CSI.VolumeHandle != volumeID {
			continue
		}

		return volumeMetadata{
			mode:          extractVolumeMode(pv.Spec.VolumeMode),
			capacityBytes: capacityFromPV(pv),
		}, nil
	}

	klog.Infof("hydrateVolumeMetadataFromCluster: volume %s not found in PV list", volumeID)
	return volumeMetadata{}, fmt.Errorf("volume %s not found", volumeID)
}

func extractVolumeMode(mode *corev1.PersistentVolumeMode) string {
	if mode == nil {
		return volumeModeFilesystem
	}
	if *mode == corev1.PersistentVolumeBlock {
		return volumeModeBlock
	}
	return volumeModeFilesystem
}

func capacityFromPV(pv *corev1.PersistentVolume) int64 {
	if pv == nil {
		return 0
	}
	quantity, ok := pv.Spec.Capacity[corev1.ResourceStorage]
	if !ok {
		return 0
	}
	return quantity.Value()
}

func isBlockMode(mode string) bool {
	return mode == volumeModeBlock
}
