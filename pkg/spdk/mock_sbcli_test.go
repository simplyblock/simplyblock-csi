package spdk

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	sanityClusterID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	sanityPoolName  = "test-pool"
	sanityPoolUUID  = "11111111-1111-1111-1111-111111111111"
	sanitySecret    = "test-secret"
)

type mockVolume struct {
	UUID   string
	Name   string
	Size   int64
	Status string // defaults to "online" when empty
}

// status returns the volume's reported status, defaulting to "online".
func (v *mockVolume) status() string {
	if v.Status == "" {
		return "online"
	}
	return v.Status
}

type mockSnapshot struct {
	UUID      string
	Name      string
	VolUUID   string
	Size      int64
	CreatedAt string
}

type mockSBCLI struct {
	srv       *httptest.Server
	mu        sync.Mutex
	volumes   map[string]*mockVolume
	snapshots map[string]*mockSnapshot

	// failCreateOnce, when true, makes the next createVolume call respond with
	// HTTP 500 without persisting the lvol. This mimics the control plane
	// failing volume creation: the lvol never comes into existence, yet a
	// broken PersistentVolume can be left dangling on the Kubernetes side.
	failCreateOnce bool

	// snapshotCreateStatus, when non-zero, makes every snapshot POST respond
	// with this HTTP status without persisting. Used to model permanent
	// control-plane errors (e.g. HTTP 400) during snapshot creation.
	snapshotCreateStatus int

	// strictSnapshotNameConflict makes the snapshot POST return HTTP 409 for any
	// existing snapshot that shares the requested name, regardless of its source
	// volume — the non-idempotent behavior the real web API exhibits under load.
	strictSnapshotNameConflict bool

	// allowDuplicateNames disables the create-volume name-conflict check, modeling
	// a control plane that does NOT enforce volume-name uniqueness. Under this
	// mode a driver that relies on the control plane for idempotency leaks a new
	// volume on every retry.
	allowDuplicateNames bool

	// failGetVolume makes GET .../volumes/{id}/ respond with HTTP 500. The
	// controller's publishVolume step is a GET, so this fails CreateVolume
	// *after* the volume has already been created.
	failGetVolume bool

	// failIf, when set, is consulted on every request before its handler runs;
	// returning true makes the mock respond HTTP 500 without running the handler.
	// It lets a test fail every API except a chosen one — a future-proof way to
	// assert an RPC performs no ordering-sensitive call (e.g. that CreateSnapshot
	// creates the snapshot only after every other API call has succeeded).
	failIf func(r *http.Request) bool

	// injectStatus, when set, is consulted before each handler; a non-zero return
	// makes the mock respond with that HTTP status without running the handler.
	// It lets a test drive an RPC through every control-plane response and assert
	// the resulting gRPC code.
	injectStatus func(r *http.Request) int
}

func newMockSBCLI() *mockSBCLI {
	m := &mockSBCLI{
		volumes:   make(map[string]*mockVolume),
		snapshots: make(map[string]*mockSnapshot),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v2/clusters/{clusterID}/storage-pools/", m.locked(m.handleListPools))
	mux.HandleFunc("GET /api/v2/clusters/{clusterID}/storage-pools/{poolID}/volumes/", m.locked(m.handleListVolumes))
	mux.HandleFunc("POST /api/v2/clusters/{clusterID}/storage-pools/{poolID}/volumes/", m.locked(m.createVolume))
	mux.HandleFunc("GET /api/v2/clusters/{clusterID}/storage-pools/{poolID}/volumes/{volumeID}/", m.locked(m.handleGetVolume))
	mux.HandleFunc("DELETE /api/v2/clusters/{clusterID}/storage-pools/{poolID}/volumes/{volumeID}/", m.locked(m.handleDeleteVolume))
	mux.HandleFunc("PUT /api/v2/clusters/{clusterID}/storage-pools/{poolID}/volumes/{volumeID}/", m.locked(m.handleResizeVolume))
	mux.HandleFunc("GET /api/v2/clusters/{clusterID}/storage-pools/{poolID}/volumes/{volumeID}/connect", m.locked(m.handleVolumeConnect))
	mux.HandleFunc("POST /api/v2/clusters/{clusterID}/storage-pools/{poolID}/volumes/{volumeID}/snapshots", m.locked(m.handleCreateSnapshot))
	mux.HandleFunc("POST /api/v2/clusters/{clusterID}/storage-pools/{poolID}/volumes/{volumeID}/clone", m.locked(m.handleCloneVolume))
	mux.HandleFunc("GET /api/v2/clusters/{clusterID}/storage-pools/{poolID}/snapshots/", m.locked(m.handleListSnapshots))
	mux.HandleFunc("DELETE /api/v2/clusters/{clusterID}/storage-pools/{poolID}/snapshots/{snapshotID}/", m.locked(m.handleDeleteSnapshot))

	m.srv = httptest.NewServer(mux)
	return m
}

func (m *mockSBCLI) URL() string { return m.srv.URL }
func (m *mockSBCLI) Close()      { m.srv.Close() }

// locked wraps a handler so it holds the mock's mutex for the duration of the call.
func (m *mockSBCLI) locked(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		defer m.mu.Unlock()
		if m.failIf != nil && m.failIf(r) {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"detail": "injected failure"})
			return
		}
		if m.injectStatus != nil {
			if code := m.injectStatus(r); code != 0 {
				writeJSON(w, code, map[string]string{"detail": "injected status"})
				return
			}
		}
		h(w, r)
	}
}

// writeJSON writes a JSON-encoded value with the given status code.
func writeJSON(w http.ResponseWriter, status int, v any) {
	b, _ := json.Marshal(v)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(b)
}

// lookupVolume returns the volume or writes a 404 and returns nil.
func (m *mockSBCLI) lookupVolume(w http.ResponseWriter, volumeID string) *mockVolume {
	volume := m.volumes[volumeID]
	if volume == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"detail": "LVol " + volumeID + " not found"})
	}
	return volume
}

// lookupSnapshot returns the snapshot or writes a 404 and returns nil.
func (m *mockSBCLI) lookupSnapshot(w http.ResponseWriter, snapshotID string) *mockSnapshot {
	snapshot := m.snapshots[snapshotID]
	if snapshot == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"detail": "snapshot " + snapshotID + " not found"})
	}
	return snapshot
}

func (m *mockSBCLI) handleListPools(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, []map[string]string{
		{"name": sanityPoolName, "id": sanityPoolUUID},
	})
}

func (m *mockSBCLI) handleListVolumes(w http.ResponseWriter, _ *http.Request) {
	list := make([]map[string]any, 0, len(m.volumes))
	for _, volume := range m.volumes {
		list = append(list, map[string]any{
			"id": volume.UUID, "name": volume.Name, "size": volume.Size, "status": volume.status(),
		})
	}
	writeJSON(w, http.StatusOK, list)
}

func (m *mockSBCLI) handleGetVolume(w http.ResponseWriter, r *http.Request) {
	if m.failGetVolume {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"detail": "get volume failed"})
		return
	}
	volume := m.lookupVolume(w, r.PathValue("volumeID"))
	if volume == nil {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"id": volume.UUID, "name": volume.Name, "size": volume.Size, "status": volume.status(),
	})
}

func (m *mockSBCLI) handleDeleteVolume(w http.ResponseWriter, r *http.Request) {
	volumeID := r.PathValue("volumeID")
	if m.lookupVolume(w, volumeID) == nil {
		return
	}
	delete(m.volumes, volumeID)
	w.WriteHeader(http.StatusNoContent)
}

func (m *mockSBCLI) handleResizeVolume(w http.ResponseWriter, r *http.Request) {
	volume := m.lookupVolume(w, r.PathValue("volumeID"))
	if volume == nil {
		return
	}
	var body struct {
		Size int64 `json:"size"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	if body.Size > 0 {
		volume.Size = body.Size
	}
	w.WriteHeader(http.StatusNoContent)
}

func (m *mockSBCLI) handleVolumeConnect(w http.ResponseWriter, r *http.Request) {
	volumeID := r.PathValue("volumeID")
	if m.lookupVolume(w, volumeID) == nil {
		return
	}
	writeJSON(w, http.StatusOK, []map[string]any{{
		"nqn":             "nqn.2023-01.io.simplyblock:test:" + volumeID,
		"transport":       "tcp",
		"ip":              "127.0.0.1",
		"port":            4420,
		"ns_id":           1,
		"reconnect-delay": 10,
		"ctrl-loss-tmo":   60,
	}})
}

func (m *mockSBCLI) handleCreateSnapshot(w http.ResponseWriter, r *http.Request) {
	volumeID := r.PathValue("volumeID")
	volume := m.lookupVolume(w, volumeID)
	if volume == nil {
		return
	}
	var body struct {
		Name string `json:"name"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)

	// Simulate a permanent control-plane rejection (e.g. HTTP 400) without
	// persisting anything.
	if m.snapshotCreateStatus != 0 {
		writeJSON(w, m.snapshotCreateStatus, map[string]string{"detail": "snapshot create rejected"})
		return
	}

	// Idempotency: check if a snapshot with this name already exists.
	for _, snapshot := range m.snapshots {
		if snapshot.Name != body.Name {
			continue
		}
		if snapshot.VolUUID == volumeID && !m.strictSnapshotNameConflict {
			// Same source → idempotent success, return existing location.
			w.Header().Set("Location", fmt.Sprintf("/api/v2/clusters/%s/storage-pools/%s/snapshots/%s/",
				sanityClusterID, sanityPoolUUID, snapshot.UUID))
			w.WriteHeader(http.StatusCreated)
			return
		}
		// Different source (or strict mode: any duplicate name) → conflict.
		writeJSON(w, http.StatusConflict, map[string]string{"detail": "snapshot name already exists"})
		return
	}

	snapshotID := uuid.New().String()
	m.snapshots[snapshotID] = &mockSnapshot{UUID: snapshotID, Name: body.Name, VolUUID: volumeID, Size: volume.Size, CreatedAt: time.Now().UTC().Format(time.RFC3339Nano)}
	w.Header().Set("Location", fmt.Sprintf("/api/v2/clusters/%s/storage-pools/%s/snapshots/%s/",
		sanityClusterID, sanityPoolUUID, snapshotID))
	w.WriteHeader(http.StatusCreated)
}

func (m *mockSBCLI) handleCloneVolume(w http.ResponseWriter, r *http.Request) {
	volume := m.lookupVolume(w, r.PathValue("volumeID"))
	if volume == nil {
		return
	}
	cloneName := r.URL.Query().Get("clone_name")
	newID := uuid.New().String()
	m.volumes[newID] = &mockVolume{UUID: newID, Name: cloneName, Size: volume.Size}
	w.Header().Set("Location", fmt.Sprintf("/api/v2/clusters/%s/storage-pools/%s/volumes/%s/",
		sanityClusterID, sanityPoolUUID, newID))
	w.WriteHeader(http.StatusCreated)
}

func (m *mockSBCLI) handleListSnapshots(w http.ResponseWriter, _ *http.Request) {
	list := make([]map[string]any, 0, len(m.snapshots))
	for _, snapshot := range m.snapshots {
		lvolURL := fmt.Sprintf("/api/v2/clusters/%s/storage-pools/%s/volumes/%s/",
			sanityClusterID, sanityPoolUUID, snapshot.VolUUID)
		list = append(list, map[string]any{
			"id": snapshot.UUID, "name": snapshot.Name, "size": snapshot.Size, "lvol": lvolURL,
			"created_at": snapshot.CreatedAt,
		})
	}
	// Sort by id for deterministic pagination.
	sort.Slice(list, func(i, j int) bool {
		return list[i]["id"].(string) < list[j]["id"].(string)
	})
	writeJSON(w, http.StatusOK, list)
}

func (m *mockSBCLI) handleDeleteSnapshot(w http.ResponseWriter, r *http.Request) {
	snapshotID := r.PathValue("snapshotID")
	if m.lookupSnapshot(w, snapshotID) == nil {
		return
	}
	delete(m.snapshots, snapshotID)
	w.WriteHeader(http.StatusNoContent)
}

// createVolume handles POST .../volumes/ for both plain create and clone-from-snapshot.
func (m *mockSBCLI) createVolume(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name       string `json:"name"`
		Size       string `json:"size"`
		SnapshotID string `json:"snapshot_id"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)

	if !m.allowDuplicateNames {
		for _, volume := range m.volumes {
			if volume.Name == body.Name {
				writeJSON(w, http.StatusConflict, map[string]string{
					"detail": "Volume " + body.Name + " exists",
				})
				return
			}
		}
	}

	// Default to 1 GiB; override with explicit size if provided.
	size := int64(1 * 1024 * 1024 * 1024)
	if body.Size != "" {
		if parsed, err := strconv.ParseInt(body.Size, 10, 64); err == nil && parsed > 0 {
			size = parsed
		}
	}
	if body.SnapshotID != "" {
		snapshot := m.snapshots[body.SnapshotID]
		if snapshot == nil {
			writeJSON(w, http.StatusNotFound, map[string]string{"detail": "snapshot " + body.SnapshotID + " not found"})
			return
		}
		// Clone from snapshot: inherit snapshot size unless explicitly overridden.
		if body.Size == "" {
			size = snapshot.Size
		}
	}

	newID := uuid.New().String()

	// Simulate the control plane failing the create request without persisting
	// the lvol: the client sees an error and no volume comes into existence.
	if m.failCreateOnce {
		m.failCreateOnce = false
		writeJSON(w, http.StatusInternalServerError, map[string]string{"detail": "internal error creating volume"})
		return
	}

	m.volumes[newID] = &mockVolume{UUID: newID, Name: body.Name, Size: size}
	w.Header().Set("Location", fmt.Sprintf("/api/v2/clusters/%s/storage-pools/%s/volumes/%s/",
		sanityClusterID, sanityPoolUUID, newID))
	w.WriteHeader(http.StatusCreated)
}
