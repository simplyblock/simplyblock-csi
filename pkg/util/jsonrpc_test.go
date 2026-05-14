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
