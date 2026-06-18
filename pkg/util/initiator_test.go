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

// whitebox test of some functions in initiator.go
package util

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

func TestExecWithTimeoutPositive(t *testing.T) {
	elapsed, err := runExecWithTimeout([]string{"true"}, 10)
	if err != nil {
		t.Fatal("should succeed")
	}
	if elapsed > 3 {
		t.Fatal("timeout error")
	}
}

func TestExecWithTimeoutNegative(t *testing.T) {
	elapsed, err := runExecWithTimeout([]string{"false"}, 10)
	if err == nil {
		t.Fatal("should fail")
	}
	if elapsed > 3 {
		t.Fatal("timeout error")
	}
}

func TestExecWithTimeoutTimeout(t *testing.T) {
	elapsed, err := runExecWithTimeout([]string{"sleep", "10"}, 1)
	if err == nil {
		t.Fatal("should fail")
	}
	if elapsed > 3 {
		t.Fatal("timeout error")
	}
}

func runExecWithTimeout(cmdLine []string, timeout int) (int, error) {
	start := time.Now()
	err := execWithTimeout(context.Background(), cmdLine, timeout)
	elapsed := int(time.Since(start) / time.Second)
	return elapsed, err
}

// writeTempFile creates a temp file with the given content and registers cleanup.
func writeTempFile(t *testing.T, content string) string {
	t.Helper()
	f, err := os.CreateTemp("", "spdkcsi-test-*")
	if err != nil {
		t.Fatalf("creating temp file: %v", err)
	}
	t.Cleanup(func() { os.Remove(f.Name()) })
	if _, err := f.WriteString(content); err != nil {
		t.Fatalf("writing temp file: %v", err)
	}
	f.Close()
	return f.Name()
}

const testSecretJSON = `{"clusters":[{"cluster_id":"test-cluster","cluster_endpoint":"http://localhost","cluster_secret":"static-secret"}]}`
const testSecretNoCredJSON = `{"clusters":[{"cluster_id":"test-cluster","cluster_endpoint":"http://localhost","cluster_secret":""}]}`

// TestCredentialAPITokenUsed verifies that when SPDKCSI_API_TOKEN_PATH points to a
// file containing a valid token, that token is used as the credential instead
// of the cluster_secret from the secret file.
func TestCredentialAPITokenUsed(t *testing.T) {
	secretFile := writeTempFile(t, testSecretJSON)
	tokenFile := writeTempFile(t, "sa-jwt-token")
	t.Setenv("SPDKCSI_SECRET", secretFile)
	t.Setenv("SPDKCSI_API_TOKEN_PATH", tokenFile)

	node, err := NewsimplyBlockClient(context.Background(), "test-cluster", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if node.API.Credential != "sa-jwt-token" {
		t.Errorf("expected API token %q as credential, got %q", "sa-jwt-token", node.API.Credential)
	}
}

// TestCredentialClusterSecretFallback verifies that when SPDKCSI_API_TOKEN_PATH is
// not set, the cluster_secret from the secret file is used unchanged.
func TestCredentialClusterSecretFallback(t *testing.T) {
	secretFile := writeTempFile(t, testSecretJSON)
	t.Setenv("SPDKCSI_SECRET", secretFile)
	t.Setenv("SPDKCSI_API_TOKEN_PATH", "")

	node, err := NewsimplyBlockClient(context.Background(), "test-cluster", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if node.API.Credential != "static-secret" {
		t.Errorf("expected cluster_secret %q, got %q", "static-secret", node.API.Credential)
	}
}

// TestCredentialAPITokenWhitespaceTrimmed verifies that leading/trailing
// whitespace in the API token file is stripped before use.
func TestCredentialAPITokenWhitespaceTrimmed(t *testing.T) {
	secretFile := writeTempFile(t, testSecretJSON)
	tokenFile := writeTempFile(t, " tok \n")
	t.Setenv("SPDKCSI_SECRET", secretFile)
	t.Setenv("SPDKCSI_API_TOKEN_PATH", tokenFile)

	node, err := NewsimplyBlockClient(context.Background(), "test-cluster", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if node.API.Credential != "tok" {
		t.Errorf("expected trimmed token %q, got %q", "tok", node.API.Credential)
	}
}

// TestCredentialAPITokenWithEmptyClusterSecret verifies that API token auth
// succeeds even when cluster_secret is empty in the secret file.
func TestCredentialAPITokenWithEmptyClusterSecret(t *testing.T) {
	secretFile := writeTempFile(t, testSecretNoCredJSON)
	tokenFile := writeTempFile(t, "sa-jwt-token")
	t.Setenv("SPDKCSI_SECRET", secretFile)
	t.Setenv("SPDKCSI_API_TOKEN_PATH", tokenFile)

	node, err := NewsimplyBlockClient(context.Background(), "test-cluster", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if node.API.Credential != "sa-jwt-token" {
		t.Errorf("expected API token %q, got %q", "sa-jwt-token", node.API.Credential)
	}
}

// TestCredentialBothMissingReturnsError verifies that when SPDKCSI_API_TOKEN_PATH is
// unset and cluster_secret is empty, NewsimplyBlockClient returns an error.
func TestCredentialBothMissingReturnsError(t *testing.T) {
	secretFile := writeTempFile(t, testSecretNoCredJSON)
	t.Setenv("SPDKCSI_SECRET", secretFile)
	t.Setenv("SPDKCSI_API_TOKEN_PATH", "")

	_, err := NewsimplyBlockClient(context.Background(), "test-cluster", "")
	if err == nil {
		t.Fatal("expected error when both cluster_secret and API token are missing, got nil")
	}
	const want = "no cluster_secret and no API token available"
	if !strings.Contains(err.Error(), want) {
		t.Errorf("error %q does not contain %q", err.Error(), want)
	}
}

// TestCredentialAPITokenFileUnreadableFallsBackToClusterSecret verifies that when
// SPDKCSI_API_TOKEN_PATH points to a nonexistent file, the driver falls back to
// cluster_secret rather than failing silently or crashing.
func TestCredentialAPITokenFileUnreadableFallsBackToClusterSecret(t *testing.T) {
	secretFile := writeTempFile(t, testSecretJSON)
	t.Setenv("SPDKCSI_SECRET", secretFile)
	t.Setenv("SPDKCSI_API_TOKEN_PATH", "/nonexistent/path/to/token")

	node, err := NewsimplyBlockClient(context.Background(), "test-cluster", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if node.API.Credential != "static-secret" {
		t.Errorf("expected fallback to cluster_secret %q, got %q", "static-secret", node.API.Credential)
	}
}

// TestCredentialAPITokenFileEmptyFallsBackToClusterSecret verifies that when
// SPDKCSI_API_TOKEN_PATH points to a file that is empty (or whitespace-only),
// the driver falls back to cluster_secret.
func TestCredentialAPITokenFileEmptyFallsBackToClusterSecret(t *testing.T) {
	secretFile := writeTempFile(t, testSecretJSON)
	tokenFile := writeTempFile(t, "   \n")
	t.Setenv("SPDKCSI_SECRET", secretFile)
	t.Setenv("SPDKCSI_API_TOKEN_PATH", tokenFile)

	node, err := NewsimplyBlockClient(context.Background(), "test-cluster", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if node.API.Credential != "static-secret" {
		t.Errorf("expected fallback to cluster_secret %q, got %q", "static-secret", node.API.Credential)
	}
}
