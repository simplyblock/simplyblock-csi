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
