package util

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestCallSBCLIUsesBearerAuthForAPIV2(t *testing.T) {
	var gotAuth string
	client := &RPCClient{
		ClusterID:     "cluster-id",
		ClusterIP:     "http://api.example.com",
		ClusterSecret: "cluster-secret",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			gotAuth = r.Header.Get("Authorization")
			return jsonResponse(), nil
		})},
	}

	if _, err := client.CallSBCLI(http.MethodGet, "/api/v2/clusters/cluster-id/storage-pools/", nil); err != nil {
		t.Fatalf("CallSBCLI: %v", err)
	}
	if gotAuth != "Bearer cluster-secret" {
		t.Fatalf("Authorization = %q, want Bearer cluster-secret", gotAuth)
	}
}

func TestCallSBCLIUsesLegacyAuthOutsideAPIV2(t *testing.T) {
	var gotAuth string
	client := &RPCClient{
		ClusterID:     "cluster-id",
		ClusterIP:     "http://api.example.com",
		ClusterSecret: "cluster-secret",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			gotAuth = r.Header.Get("Authorization")
			return jsonResponse(), nil
		})},
	}

	if _, err := client.CallSBCLI(http.MethodGet, "/lvol/lvol-id", nil); err != nil {
		t.Fatalf("CallSBCLI: %v", err)
	}
	if gotAuth != "cluster-id cluster-secret" {
		t.Fatalf("Authorization = %q, want cluster-id cluster-secret", gotAuth)
	}
}

func TestCloneVolumeUsesPostAndLocationHeader(t *testing.T) {
	var gotMethod string
	var gotPath string
	var gotQuery string
	client := &RPCClient{
		ClusterID:     "cluster-id",
		PoolID:        "pool-id",
		ClusterIP:     "http://api.example.com",
		ClusterSecret: "cluster-secret",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
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
	}

	cloneID, err := client.cloneVolume("source-id", "clone name", "1073741824", "default/my-pvc")
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
