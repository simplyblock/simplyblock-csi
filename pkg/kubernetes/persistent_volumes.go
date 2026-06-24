package kubernetes

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog"

	"github.com/spdk/spdk-csi/pkg/kubernetes/volumehandle"
)

// indexPersistentVolumeByCSIDriver is the IndexFunc that groups
// PersistentVolumes by their CSI driver name.
func indexPersistentVolumeByCSIDriver(obj interface{}) ([]string, error) {
	pv, ok := obj.(*corev1.PersistentVolume)
	if !ok || pv.Spec.CSI == nil {
		return nil, nil
	}
	return []string{pv.Spec.CSI.Driver}, nil
}

// indexPersistentVolumeByLvolID is the IndexFunc that maps PersistentVolumes by
// the lvol ID in their CSI volume handle ("<clusterID>:<poolID>:<lvolID>").
// Handles that are not well-formed are left out of the index.
func indexPersistentVolumeByLvolID(obj interface{}) ([]string, error) {
	pv, ok := obj.(*corev1.PersistentVolume)
	if !ok || pv.Spec.CSI == nil {
		return nil, nil
	}
	vh, ok := volumehandle.Parse(pv.Spec.CSI.VolumeHandle)
	if !ok {
		return nil, nil
	}
	return []string{vh.VolumeID}, nil
}

// PersistentVolumesByDriver returns every PersistentVolume provisioned by the
// given CSI driver. It is served from the cache when synced, otherwise listed
// directly from the API and filtered client-side (the API server has no
// spec.csi.driver field selector for PVs). Returns nil on a nil Manager.
func (m *Manager) PersistentVolumesByDriver(ctx context.Context, driver string) ([]*corev1.PersistentVolume, error) {
	if m == nil {
		return nil, nil
	}

	if m.pvInformer.HasSynced() {
		objs, err := m.pvInformer.GetIndexer().ByIndex(csiDriverIndex, driver)
		if err == nil {
			pvs := make([]*corev1.PersistentVolume, 0, len(objs))
			for _, obj := range objs {
				if pv, ok := obj.(*corev1.PersistentVolume); ok {
					pvs = append(pvs, pv)
				}
			}
			return pvs, nil
		}
		klog.Warningf("kubernetes cache manager: %q index lookup failed, falling back to API: %v", csiDriverIndex, err)
	}

	list, err := m.client.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	pvs := make([]*corev1.PersistentVolume, 0, len(list.Items))
	for i := range list.Items {
		pv := &list.Items[i]
		if pv.Spec.CSI != nil && pv.Spec.CSI.Driver == driver {
			pvs = append(pvs, pv)
		}
	}
	return pvs, nil
}

// PersistentVolumeByName returns the PersistentVolume with the given name.
// PersistentVolumes are cluster-scoped, so the name alone is the cache key.
// Served from the cache when synced, otherwise fetched directly from the API.
// A missing PV is reported as a NotFound error from either path.
func (m *Manager) PersistentVolumeByName(ctx context.Context, name string) (*corev1.PersistentVolume, error) {
	if m == nil {
		return nil, apierrors.NewNotFound(corev1.Resource("persistentvolumes"), name)
	}

	if m.pvInformer.HasSynced() {
		obj, exists, err := m.pvInformer.GetStore().GetByKey(name)
		if err == nil {
			if !exists {
				return nil, apierrors.NewNotFound(corev1.Resource("persistentvolumes"), name)
			}
			if pv, ok := obj.(*corev1.PersistentVolume); ok {
				return pv, nil
			}
		} else {
			klog.Warningf("kubernetes cache manager: PersistentVolume %q lookup failed, falling back to API: %v", name, err)
		}
	}

	return m.client.CoreV1().PersistentVolumes().Get(ctx, name, metav1.GetOptions{})
}

// PersistentVolumeByLogicalVolumeID returns the PersistentVolume whose CSI
// volume handle ("<clusterID>:<poolID>:<lvolID>") carries the given lvol ID.
// Served from the cache when synced, otherwise listed from the API and matched
// client-side (the handle is not an API-queryable field). A missing or
// ambiguous lvol ID is reported as a NotFound error.
func (m *Manager) PersistentVolumeByLogicalVolumeID(ctx context.Context, lvolID string) (*corev1.PersistentVolume, error) {
	if m == nil {
		return nil, apierrors.NewNotFound(corev1.Resource("persistentvolumes"), lvolID)
	}

	if m.pvInformer.HasSynced() {
		objs, err := m.pvInformer.GetIndexer().ByIndex(lvolIndex, lvolID)
		if err == nil {
			if len(objs) == 0 {
				return nil, apierrors.NewNotFound(corev1.Resource("persistentvolumes"), lvolID)
			}
			if pv, ok := objs[0].(*corev1.PersistentVolume); ok {
				return pv, nil
			}
		} else {
			klog.Warningf("kubernetes cache manager: %q index lookup failed, falling back to API: %v", lvolIndex, err)
		}
	}

	list, err := m.client.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	for i := range list.Items {
		pv := &list.Items[i]
		if pv.Spec.CSI == nil {
			continue
		}
		if vh, ok := volumehandle.Parse(pv.Spec.CSI.VolumeHandle); ok && vh.VolumeID == lvolID {
			return pv, nil
		}
	}
	return nil, apierrors.NewNotFound(corev1.Resource("persistentvolumes"), lvolID)
}
