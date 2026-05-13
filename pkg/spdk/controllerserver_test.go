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

package spdk

import (
	"testing"

	"github.com/spdk/spdk-csi/pkg/util"
)

func TestApplyLvolAddAnnotationOverrides(t *testing.T) {
	req := util.CreateLVolData{
		LvolName:     "sc-name",
		Size:         "1073741824",
		LvsName:      "sc-pool",
		Fabric:       "tcp",
		Compression:  false,
		Encryption:   false,
		Replicate:    false,
		MaxRWIOPS:    "1",
		MaxRWmBytes:  "2",
		MaxRmBytes:   "3",
		MaxWmBytes:   "4",
		MaxSize:      "5",
		MaxNamespace: 1,
		DistNdcs:     1,
		DistNpcs:     1,
		PriorClass:   0,
		CryptoKey1:   "sc-key1",
		CryptoKey2:   "sc-key2",
		HostID:       "sc-host",
		LvolID:       "sc-uid",
		Namespaced:   false,
		PvcName:      "default/pvc",
	}

	annotations := map[string]string{
		annotationLvolName:     "ann-name",
		annotationLvolSize:     "2147483648",
		annotationPool:         "ann-pool",
		annotationFabric:       "rdma",
		annotationCompression:  "true",
		annotationEncryption:   "true",
		annotationReplicate:    "true",
		annotationMaxRWIOPS:    "11",
		annotationMaxRWmBytes:  "12",
		annotationMaxRmBytes:   "13",
		annotationMaxWmBytes:   "14",
		annotationMaxSize:      "15",
		annotationMaxNamespace: "2",
		annotationDistNdcs:     "3",
		annotationDistNpcs:     "4",
		annotationPriorClass:   "1",
		annotationCryptoKey1:   "ann-key1",
		annotationCryptoKey2:   "ann-key2",
		annotationHostID:       "ann-host",
		annotationLvolUID:      "ann-uid",
		annotationNamespaced:   "false",
		annotationPVCName:      "other/pvc",
	}

	if err := applyLvolAddAnnotationOverrides(&req, annotations); err != nil {
		t.Fatalf("applyLvolAddAnnotationOverrides() error = %v", err)
	}

	if req.LvolName != "ann-name" ||
		req.Size != "2147483648" ||
		req.LvsName != "ann-pool" ||
		req.Fabric != "rdma" ||
		!req.Compression ||
		!req.Encryption ||
		!req.Replicate ||
		req.MaxRWIOPS != "11" ||
		req.MaxRWmBytes != "12" ||
		req.MaxRmBytes != "13" ||
		req.MaxWmBytes != "14" ||
		req.MaxSize != "15" ||
		req.MaxNamespace != 2 ||
		req.DistNdcs != 3 ||
		req.DistNpcs != 4 ||
		req.PriorClass != 1 ||
		req.CryptoKey1 != "ann-key1" ||
		req.CryptoKey2 != "ann-key2" ||
		req.HostID != "ann-host" ||
		req.LvolID != "ann-uid" ||
		req.Namespaced ||
		req.PvcName != "other/pvc" {
		t.Fatalf("annotation overrides were not fully applied: %+v", req)
	}
}

func TestApplyLvolAddAnnotationOverridesSupportsExistingAliases(t *testing.T) {
	req := util.CreateLVolData{}
	annotations := map[string]string{
		annotationQoSRWIOPS:           "21",
		deprecatedAnnotationQoSRWMBps: "22",
		annotationQoSRMBps:            "23",
		deprecatedAnnotationQoSWMBps:  "24",
		deprecatedAnnotationHostID:    "host-alias",
		deprecatedAnnotationLvolID:    "uid-alias",
	}

	if err := applyLvolAddAnnotationOverrides(&req, annotations); err != nil {
		t.Fatalf("applyLvolAddAnnotationOverrides() error = %v", err)
	}

	if req.MaxRWIOPS != "21" ||
		req.MaxRWmBytes != "22" ||
		req.MaxRmBytes != "23" ||
		req.MaxWmBytes != "24" ||
		req.HostID != "host-alias" ||
		req.LvolID != "uid-alias" {
		t.Fatalf("annotation aliases were not applied: %+v", req)
	}
}

func TestApplyLvolAddAnnotationOverridesRejectsInvalidTypedValues(t *testing.T) {
	tests := []struct {
		name       string
		annotation map[string]string
	}{
		{
			name:       "invalid int",
			annotation: map[string]string{annotationMaxNamespace: "not-int"},
		},
		{
			name:       "invalid bool",
			annotation: map[string]string{annotationCompression: "not-bool"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := applyLvolAddAnnotationOverrides(&util.CreateLVolData{}, tt.annotation); err == nil {
				t.Fatalf("applyLvolAddAnnotationOverrides() error = nil, want error")
			}
		})
	}
}
