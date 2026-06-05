package util

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

// Shared test fixtures.
const (
	testPool1UUID = "7f76add2-ede2-451a-b942-3980268e444b"
	testPool1Name = "pool-one"
	testPool2UUID = "cd8cac08-dd13-46ab-853c-4495227dcdfb"
	testPool2Name = "pool-two"
	testVolUUID   = "98747845-d746-4035-9ebd-a3991c4476dd"
)

func newTestClient(transport roundTripFunc) *APIClient {
	return &APIClient{
		ClusterID:     "cluster-id",
		ClusterSecret: "cluster-secret",
		conn: &Connection{
			Endpoint: "http://api.example.com",
			HTTP:     &http.Client{Transport: transport},
		},
	}
}

// poolUUIDFromPath extracts the pool UUID from a v2 volume path of the form
// .../storage-pools/{poolUUID}/volumes/...
func poolUUIDFromPath(path string) string {
	_, after, ok := strings.Cut(path, "/storage-pools/")
	if !ok {
		return ""
	}
	uuid, _, _ := strings.Cut(after, "/")
	return uuid
}

// newFindPoolTransport returns a roundTripFunc
func newFindPoolTransport(pools []StoragePool, volumeInPool, lvolID string, unexpectedErrPools map[string]bool) roundTripFunc {
	return func(r *http.Request) (*http.Response, error) {
		if strings.Contains(r.URL.Path, "/volumes/") {
			poolUUID := poolUUIDFromPath(r.URL.Path)
			if unexpectedErrPools[poolUUID] {
				return &http.Response{
					StatusCode: http.StatusInternalServerError,
					Body:       io.NopCloser(strings.NewReader(`{"detail":"Internal Server Error"}`)),
					Header:     make(http.Header),
				}, nil
			}
			if poolUUID == volumeInPool {
				body, _ := json.Marshal(LvolResp{UUID: lvolID, LvolSize: 1 << 30, Status: "online"})
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(bytes.NewReader(body)),
					Header:     make(http.Header),
				}, nil
			}
			return &http.Response{
				StatusCode: http.StatusNotFound,
				Body:       io.NopCloser(strings.NewReader(`{"detail":"LVol ` + lvolID + ` not found"}`)),
				Header:     make(http.Header),
			}, nil
		}
		body, _ := json.Marshal(pools)
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewReader(body)),
			Header:     make(http.Header),
		}, nil
	}
}

func TestDoUsesBearerAuthForAPIV2(t *testing.T) {
	var gotAuth string
	client := newTestClient(func(r *http.Request) (*http.Response, error) {
		gotAuth = r.Header.Get("Authorization")
		return jsonResponse(), nil
	})

	if _, err := client.do(context.Background(), http.MethodGet, "/api/v2/clusters/cluster-id/storage-pools/", nil); err != nil {
		t.Fatalf("do: %v", err)
	}
	if gotAuth != "Bearer cluster-secret" {
		t.Fatalf("Authorization = %q, want Bearer cluster-secret", gotAuth)
	}
}

func TestDoUsesLegacyAuthOutsideAPIV2(t *testing.T) {
	var gotAuth string
	client := newTestClient(func(r *http.Request) (*http.Response, error) {
		gotAuth = r.Header.Get("Authorization")
		return jsonResponse(), nil
	})

	if _, err := client.do(context.Background(), http.MethodGet, "/lvol/lvol-id", nil); err != nil {
		t.Fatalf("do: %v", err)
	}
	if gotAuth != "cluster-id cluster-secret" {
		t.Fatalf("Authorization = %q, want cluster-id cluster-secret", gotAuth)
	}
}

func TestCloneVolumeUsesPostAndLocationHeader(t *testing.T) {
	var gotMethod string
	var gotPath string
	var gotQuery string
	client := newTestClient(func(r *http.Request) (*http.Response, error) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotQuery = r.URL.RawQuery
		return &http.Response{
			StatusCode: http.StatusCreated,
			Body:       io.NopCloser(strings.NewReader("")),
			Header: http.Header{
				"Location": []string{"/api/v2/clusters/cluster-id/storage-pools/pool-id/volumes/clone-id/"},
			},
		}, nil
	})

	cloneID, err := client.cloneVolume(context.Background(), "pool-id", "source-id", "clone name", "1073741824", "default/my-pvc")
	if err != nil {
		t.Fatalf("cloneVolume: %v", err)
	}
	if cloneID != "clone-id" {
		t.Fatalf("cloneID = %q, want clone-id", cloneID)
	}
	if gotMethod != http.MethodPost {
		t.Fatalf("method = %q, want POST", gotMethod)
	}
	if gotPath != "/api/v2/clusters/cluster-id/storage-pools/pool-id/volumes/source-id/clone" {
		t.Fatalf("path = %q", gotPath)
	}
	if !strings.Contains(gotQuery, "clone_name=clone+name") ||
		!strings.Contains(gotQuery, "new_size=1073741824") ||
		!strings.Contains(gotQuery, "pvc_name=default%2Fmy-pvc") {
		t.Fatalf("query = %q", gotQuery)
	}
}

func TestFindPoolForVolume(t *testing.T) {
	twoPools := []StoragePool{
		{Name: testPool1Name, UUID: testPool1UUID},
		{Name: testPool2Name, UUID: testPool2UUID},
	}

	tests := []struct {
		name      string
		transport roundTripFunc
		wantPool  string
		wantErr   string
	}{
		{
			name:      "found in first pool",
			transport: newFindPoolTransport(twoPools, testPool1UUID, testVolUUID, nil),
			wantPool:  testPool1UUID,
		},
		{
			name:      "found in second pool",
			transport: newFindPoolTransport(twoPools, testPool2UUID, testVolUUID, nil),
			wantPool:  testPool2UUID,
		},
		{
			name:      "not found in any pool",
			transport: newFindPoolTransport(twoPools, "", testVolUUID, nil),
			wantErr:   "not found in any pool",
		},
		{
			name: "list pools error",
			transport: func(r *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusInternalServerError,
					Body:       io.NopCloser(strings.NewReader(`{"detail":"Internal Server Error"}`)),
					Header:     make(http.Header),
				}, nil
			},
			wantErr: "failed to list pools",
		},
		{
			name:      "unexpected non-404 error in pool",
			transport: newFindPoolTransport(twoPools, "", testVolUUID, map[string]bool{testPool1UUID: true}),
			wantErr:   "unexpected error searching for volume",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			poolID, err := newTestClient(tc.transport).findPoolForVolume(context.Background(), testVolUUID)

			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("findPoolForVolume() error = %v, want containing %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("findPoolForVolume() unexpected error: %v", err)
			}
			if poolID != tc.wantPool {
				t.Errorf("findPoolForVolume() poolID = %q, want %q", poolID, tc.wantPool)
			}
		})
	}
}

func TestGetPoolUUIDByName(t *testing.T) {
	pools := []StoragePool{
		{Name: testPool1Name, UUID: testPool1UUID},
		{Name: testPool2Name, UUID: testPool2UUID},
	}
	poolsJSON, _ := json.Marshal(pools)
	transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewReader(poolsJSON)),
			Header:     make(http.Header),
		}, nil
	})

	tests := []struct {
		name     string
		poolName string
		wantUUID string
		wantErr  string
	}{
		{name: "found pool-one", poolName: testPool1Name, wantUUID: testPool1UUID},
		{name: "found pool-two", poolName: testPool2Name, wantUUID: testPool2UUID},
		{name: "not found", poolName: "no-such-pool", wantErr: "not found"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			uuid, err := newTestClient(transport).getPoolUUIDByName(context.Background(), tc.poolName)

			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("getPoolUUIDByName() error = %v, want containing %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("getPoolUUIDByName() unexpected error: %v", err)
			}
			if uuid != tc.wantUUID {
				t.Errorf("getPoolUUIDByName() uuid = %q, want %q", uuid, tc.wantUUID)
			}
		})
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func jsonResponse() *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(`{"ok":true}`)),
		Header:     make(http.Header),
	}
}
