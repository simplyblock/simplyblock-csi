package kubernetes

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog"
)

// PersistentVolumeClaimByNamespaceAndName returns the PersistentVolumeClaim with
// the given namespace and name. Served from the cache when synced, otherwise
// fetched directly from the API. A missing claim is reported as a NotFound error
// from either path.
func (m *Manager) PersistentVolumeClaimByNamespaceAndName(ctx context.Context, namespace, name string) (*corev1.PersistentVolumeClaim, error) {
	if m == nil {
		return nil, apierrors.NewNotFound(corev1.Resource("persistentvolumeclaims"), name)
	}

	if m.pvcInformer.HasSynced() {
		obj, exists, err := m.pvcInformer.GetStore().GetByKey(namespace + "/" + name)
		if err == nil {
			if !exists {
				return nil, apierrors.NewNotFound(corev1.Resource("persistentvolumeclaims"), name)
			}
			if pvc, ok := obj.(*corev1.PersistentVolumeClaim); ok {
				return pvc, nil
			}
		} else {
			klog.Warningf("kubernetes cache manager: PersistentVolumeClaim %s/%s lookup failed, falling back to API: %v", namespace, name, err)
		}
	}

	return m.client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, name, metav1.GetOptions{})
}
