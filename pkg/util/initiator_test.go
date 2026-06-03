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
	"fmt"
	"os"
	"path/filepath"
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

// TestNvmeDeviceGlobsDedup verifies that nvmeDeviceGlobs deduplicates entries
// when model == uuid (the common case: own subsystem, default max_namespace_per_subsys=1).
func TestNvmeDeviceGlobsDedup(t *testing.T) {
	id := "e7a45cb7-1234-1234-1234-123456789abc"
	globs := nvmeDeviceGlobs(id, id, "1")
	if len(globs) != 2 {
		t.Fatalf("expected 2 globs when model==uuid, got %d: %v", len(globs), globs)
	}
}

// TestNvmeDeviceGlobsRace verifies that nvmeDeviceGlobs emits separate model
// and uuid entries when they differ (the namespace-sharing deletion race case).
func TestNvmeDeviceGlobsRace(t *testing.T) {
	model := "e8a1c0d6-0000-0000-0000-000000000000"
	uuid := "e7a45cb7-1111-1111-1111-111111111111"
	globs := nvmeDeviceGlobs(model, uuid, "1")
	if len(globs) != 4 {
		t.Fatalf("expected 4 globs when model!=uuid, got %d: %v", len(globs), globs)
	}
}

// TestWaitForDeviceReadyRace is the key regression test.
//
// It simulates the deletion-race scenario:
//   - model is NQN-derived and points to a deleted volume's ID (e8a1c0d6)
//   - uuid is the clone's own lvolID (e7a45cb7)
//   - the backend rebuilt the NVMe subsystem for the clone using uuid as the
//     model_number, so the device appears under *uuid*, not *model*
//
// Before the fix, Connect() only searched by model → timeout.
// After the fix, Connect() searches both model and uuid per tick → uuid glob hits.
func TestWaitForDeviceReadyRace(t *testing.T) {
	dir := t.TempDir()

	modelID := "e8a1c0d6-dead-dead-dead-deaddeaddead"
	uuidID := "e7a45cb7-cafe-cafe-cafe-cafecafecafe"
	nsId := "1"

	// Device exists only under the uuid-based name (race: backend rebuilt
	// subsystem with clone's own UUID as model_number).
	deviceFile := filepath.Join(dir, fmt.Sprintf("nvme-%s_%s", uuidID, nsId))
	if err := os.WriteFile(deviceFile, nil, 0o600); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Reproduce the pre-fix Connect() logic: single model-based glob only.
	modelGlob := filepath.Join(dir, fmt.Sprintf("*%s*_%s", modelID, nsId))
	_, errOld := waitForDeviceReady(ctx, 2, modelGlob)
	if errOld == nil {
		t.Fatal("pre-fix probe should have failed: device is not at the model path")
	}

	// Post-fix Connect() logic: model + uuid globs tried each tick.
	uuidGlob := filepath.Join(dir, fmt.Sprintf("*%s*_%s", uuidID, nsId))
	got, errNew := waitForDeviceReady(ctx, 2, modelGlob, uuidGlob)
	if errNew != nil {
		t.Fatalf("post-fix probe should have found the device: %v", errNew)
	}
	if got != deviceFile {
		t.Fatalf("expected %s, got %s", deviceFile, got)
	}
}

// TestWaitForDeviceReadyNormal verifies the common path (model == uuid, device
// present at the model path) is unaffected by the fix.
func TestWaitForDeviceReadyNormal(t *testing.T) {
	dir := t.TempDir()

	id := "e7a45cb7-cafe-cafe-cafe-cafecafecafe"
	nsId := "1"

	deviceFile := filepath.Join(dir, fmt.Sprintf("nvme-%s_%s", id, nsId))
	if err := os.WriteFile(deviceFile, nil, 0o600); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	glob := filepath.Join(dir, fmt.Sprintf("*%s*_%s", id, nsId))
	got, err := waitForDeviceReady(ctx, 2, glob)
	if err != nil {
		t.Fatalf("normal probe should succeed: %v", err)
	}
	if got != deviceFile {
		t.Fatalf("expected %s, got %s", deviceFile, got)
	}
}
