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

// whitebox tests for TLS mode parsing and transport construction in nvmf.go
package util

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestParseTLSMode(t *testing.T) {
	cases := []struct {
		in      string
		want    tlsMode
		wantErr bool
	}{
		{"", tlsDisabled, false},
		{"disabled", tlsDisabled, false},
		{"anonymous", tlsAnonymous, false},
		{"authenticated", tlsAuthenticated, false},
		{"Anonymous", tlsDisabled, true},
		{"on", tlsDisabled, true},
		{"true", tlsDisabled, true},
	}
	for _, c := range cases {
		got, err := parseTLSMode(c.in)
		if (err != nil) != c.wantErr {
			t.Errorf("parseTLSMode(%q) err=%v wantErr=%v", c.in, err, c.wantErr)
			continue
		}
		if !c.wantErr && got != c.want {
			t.Errorf("parseTLSMode(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

func writeTestPEMs(t *testing.T) (caFile, certFile, keyFile string) {
	t.Helper()
	dir := t.TempDir()

	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("genkey: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		IsCA:         true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("createcert: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		t.Fatalf("marshalkey: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	caFile = filepath.Join(dir, "ca.crt")
	certFile = filepath.Join(dir, "tls.crt")
	keyFile = filepath.Join(dir, "tls.key")
	for path, data := range map[string][]byte{caFile: certPEM, certFile: certPEM, keyFile: keyPEM} {
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
	}
	return caFile, certFile, keyFile
}

func transportOf(t *testing.T, n *NodeNVMf) *http.Transport {
	t.Helper()
	tr, ok := n.Client.HTTPClient.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport is %T, want *http.Transport", n.Client.HTTPClient.Transport)
	}
	return tr
}

func TestNewNVMfDisabled(t *testing.T) {
	t.Setenv(envTLSConnect, "disabled")
	n, err := NewNVMf("c", "http://api.example.com:5000", "s")
	if err != nil {
		t.Fatalf("NewNVMf: %v", err)
	}
	if n.Client.ClusterIP != "http://api.example.com:5000" {
		t.Errorf("ClusterIP rewritten unexpectedly: %s", n.Client.ClusterIP)
	}
	if n.Client.HTTPClient.Transport != http.DefaultTransport {
		t.Errorf("expected http.DefaultTransport, got %T", n.Client.HTTPClient.Transport)
	}
}

func TestNewNVMfAnonymous(t *testing.T) {
	caFile, _, _ := writeTestPEMs(t)
	t.Setenv(envTLSConnect, "anonymous")
	t.Setenv(envTLSCAFile, caFile)

	n, err := NewNVMf("c", "http://api.example.com:5000", "s")
	if err != nil {
		t.Fatalf("NewNVMf: %v", err)
	}
	if n.Client.ClusterIP != "https://api.example.com:5000" {
		t.Errorf("ClusterIP not rewritten to https: %s", n.Client.ClusterIP)
	}
	cfg := transportOf(t, n).TLSClientConfig
	if cfg == nil || cfg.RootCAs == nil {
		t.Fatal("expected TLSClientConfig with RootCAs")
	}
	if len(cfg.Certificates) != 0 {
		t.Errorf("anonymous mode should have no client certs, got %d", len(cfg.Certificates))
	}
}

func TestNewNVMfAuthenticated(t *testing.T) {
	caFile, certFile, keyFile := writeTestPEMs(t)
	t.Setenv(envTLSConnect, "authenticated")
	t.Setenv(envTLSCAFile, caFile)
	t.Setenv(envTLSCert, certFile)
	t.Setenv(envTLSKey, keyFile)

	n, err := NewNVMf("c", "http://api.example.com:5000", "s")
	if err != nil {
		t.Fatalf("NewNVMf: %v", err)
	}
	cfg := transportOf(t, n).TLSClientConfig
	if cfg == nil || cfg.RootCAs == nil {
		t.Fatal("expected TLSClientConfig with RootCAs")
	}
	if len(cfg.Certificates) != 1 {
		t.Errorf("authenticated mode should have 1 client cert, got %d", len(cfg.Certificates))
	}
}

func TestNewNVMfInvalidMode(t *testing.T) {
	t.Setenv(envTLSConnect, "bogus")
	if _, err := NewNVMf("c", "http://api.example.com:5000", "s"); err == nil {
		t.Fatal("expected error for invalid SB_TLS_CONNECT, got nil")
	}
}

func TestNewNVMfMissingCA(t *testing.T) {
	t.Setenv(envTLSConnect, "anonymous")
	t.Setenv(envTLSCAFile, filepath.Join(t.TempDir(), "missing.crt"))
	if _, err := NewNVMf("c", "http://api.example.com:5000", "s"); err == nil {
		t.Fatal("expected error for missing CA file, got nil")
	}
}
