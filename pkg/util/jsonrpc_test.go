package util

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestDoUsesBearerAuthForAPIV2(t *testing.T) {
	var gotAuth string
	client := &APIClient{
		ClusterID:     "cluster-id",
		ClusterSecret: "cluster-secret",
		conn: &Connection{
			Endpoint: "http://api.example.com",
			HTTP: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				gotAuth = r.Header.Get("Authorization")
				return jsonResponse(), nil
			})},
		},
	}

	if _, err := client.do(context.Background(), http.MethodGet, "/api/v2/clusters/cluster-id/storage-pools/", nil); err != nil {
		t.Fatalf("do: %v", err)
	}
	if gotAuth != "Bearer cluster-secret" {
		t.Fatalf("Authorization = %q, want Bearer cluster-secret", gotAuth)
	}
}

func TestDoUsesLegacyAuthOutsideAPIV2(t *testing.T) {
	var gotAuth string
	client := &APIClient{
		ClusterID:     "cluster-id",
		ClusterSecret: "cluster-secret",
		conn: &Connection{
			Endpoint: "http://api.example.com",
			HTTP: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				gotAuth = r.Header.Get("Authorization")
				return jsonResponse(), nil
			})},
		},
	}

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
	client := &APIClient{
		ClusterID:     "cluster-id",
		ClusterSecret: "cluster-secret",
		conn: &Connection{
			Endpoint: "http://api.example.com",
			HTTP: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
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
			})},
		},
	}

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
	const poolsPath = "/api/v2/clusters/cluster-id/storage-pools/"
	const volPath1 = "/api/v2/clusters/cluster-id/storage-pools/pool-1/volumes/vol-abc/"
	const volPath2 = "/api/v2/clusters/cluster-id/storage-pools/pool-2/volumes/vol-abc/"

	const twoPools = `[{"id":"pool-1","name":"pool-one"},{"id":"pool-2","name":"pool-two"}]`
	const onePool = `[{"id":"pool-1","name":"pool-one"}]`
	const volFound = `{"id":"vol-abc","name":"my-vol","size":1073741824,"status":"online"}`

	okResp := func(body string) *http.Response {
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}
	}
	errResp := func(status int, detail string) *http.Response {
		return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(`{"detail":"` + detail + `"}`)), Header: make(http.Header)}
	}
	newClient := func(transport roundTripFunc) *APIClient {
		return &APIClient{
			ClusterID:     "cluster-id",
			ClusterSecret: "cluster-secret",
			conn: &Connection{
				Endpoint: "http://api.example.com",
				HTTP:     &http.Client{Transport: transport},
			},
		}
	}

	tests := []struct {
		name      string
		transport roundTripFunc
		wantPool  string
		wantErr   string
	}{
		{
			name: "found in first pool",
			transport: func(r *http.Request) (*http.Response, error) {
				switch r.URL.Path {
				case poolsPath:
					return okResp(twoPools), nil
				case volPath1:
					return okResp(volFound), nil
				default:
					t.Errorf("unexpected request to %s", r.URL.Path)
					return jsonResponse(), nil
				}
			},
			wantPool: "pool-1",
		},
		{
			name: "found in second pool after 404 in first",
			transport: func(r *http.Request) (*http.Response, error) {
				switch r.URL.Path {
				case poolsPath:
					return okResp(twoPools), nil
				case volPath1:
					return errResp(http.StatusNotFound, "LVol vol-abc not found"), nil
				case volPath2:
					return okResp(volFound), nil
				default:
					t.Errorf("unexpected request to %s", r.URL.Path)
					return jsonResponse(), nil
				}
			},
			wantPool: "pool-2",
		},
		{
			name: "not found in any pool",
			transport: func(r *http.Request) (*http.Response, error) {
				if r.URL.Path == poolsPath {
					return okResp(twoPools), nil
				}
				return errResp(http.StatusNotFound, "LVol vol-abc not found"), nil
			},
			wantErr: "not found in any pool",
		},
		{
			name: "list pools error",
			transport: func(r *http.Request) (*http.Response, error) {
				return errResp(http.StatusInternalServerError, "Internal Server Error"), nil
			},
			wantErr: "failed to list pools",
		},
		{
			name: "unexpected non-404 error in pool",
			transport: func(r *http.Request) (*http.Response, error) {
				if r.URL.Path == poolsPath {
					return okResp(onePool), nil
				}
				return errResp(http.StatusInternalServerError, "Internal Server Error"), nil
			},
			wantErr: "unexpected error searching for volume",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			poolID, err := newClient(tc.transport).findPoolForVolume(context.Background(), "vol-abc")

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
