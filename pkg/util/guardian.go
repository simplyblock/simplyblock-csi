package util

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog"
)

type GuardianConfig struct {
	NodeName         string
	PollInterval     time.Duration
	RestartBackoff   time.Duration
	GraceSeconds     int64
	OptInLabelKey    string
	OptInLabelValue  string
	OptOutLabelKey   string
	OptOutLabelValue string
	DryRun           bool

	// Minimum time a lvol must remain "broken" before we restart pods after cluster is active.
	MinBrokenFor time.Duration

	StatePath string
}

type ClusterStatus struct {
	Status string `json:"status"`
}

// NewDefaultGuardianConfig returns sane defaults.
func NewDefaultGuardianConfig(nodeName string) GuardianConfig {
	return GuardianConfig{
		NodeName:         nodeName,
		PollInterval:     5 * time.Minute,
		RestartBackoff:   10 * time.Minute,
		GraceSeconds:     0,
		OptInLabelKey:    "simplyblock.io/auto-restart-on-pathloss",
		OptInLabelValue:  "true",
		OptOutLabelKey:   "simplyblock.io/guardian-disable",
		OptOutLabelValue: "true",
		DryRun:           false,
		MinBrokenFor: parseDurationFromEnv(
			"GUARDIAN_MIN_BROKEN_FOR",
			30*time.Second,
		),
		StatePath: "/var/run/simplyblock/guardian/state.json",
	}
}

type persistedLvolState struct {
	PodUIDs   []string  `json:"podUIDs,omitempty"`
	ClusterID string    `json:"clusterID,omitempty"`
	BrokenAt  time.Time `json:"brokenAt,omitempty"`
}

type guardianState struct {
	Lvols              map[string]persistedLvolState `json:"lvols"`
	LastRestart        map[string]time.Time          `json:"lastRestart,omitempty"`
	ClusterWasInactive map[string]bool               `json:"clusterWasInactive,omitempty"`
}

type LvolState struct {
	// podUID -> present
	PodUIDs map[string]struct{} `json:"-"` // persisted as []string

	// derived from NQN
	ClusterID string `json:"clusterID"`

	// zero value means "not broken"
	BrokenAt time.Time `json:"brokenAt,omitempty"`
}

// Guardian tracks which pod uses which lvol and restarts affected pods
// ONLY after cluster becomes active again.
type Guardian struct {
	cfg GuardianConfig

	cs *kubernetes.Clientset

	mu sync.Mutex

	// lvolID -> state
	lvols map[string]*LvolState

	// podUID -> last restart time
	lastRestart map[string]time.Time

	// cluster transition state
	clusterWasInactive map[string]bool
}

func (g *Guardian) loadState() {
	if g.cfg.StatePath == "" {
		return
	}

	b, err := os.ReadFile(g.cfg.StatePath)
	if err != nil {
		if os.IsNotExist(err) {
			klog.Infof("Guardian: no prior state found at %s", g.cfg.StatePath)
			return
		}
		klog.Warningf("Guardian: failed to read state file %s: %v", g.cfg.StatePath, err)
		return
	}

	var st guardianState
	if err := json.Unmarshal(b, &st); err != nil {
		klog.Warningf("Guardian: failed to unmarshal state file %s: %v", g.cfg.StatePath, err)
		return
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	if st.Lvols != nil {
		g.lvols = map[string]*LvolState{}
		for lvolID, pls := range st.Lvols {
			set := map[string]struct{}{}
			for _, uid := range pls.PodUIDs {
				if uid == "" {
					continue
				}
				set[uid] = struct{}{}
			}
			g.lvols[lvolID] = &LvolState{
				PodUIDs:   set,
				ClusterID: pls.ClusterID,
				BrokenAt:  pls.BrokenAt,
			}
		}
	}

	if st.LastRestart != nil {
		g.lastRestart = st.LastRestart
	}

	if st.ClusterWasInactive != nil {
		g.clusterWasInactive = st.ClusterWasInactive
	}

	klog.Infof("Guardian: loaded state: lvols=%d lastRestart=%d clusterWasInactive=%d",
		len(g.lvols), len(g.lastRestart), len(g.clusterWasInactive),
	)
}

// StartGuardian starts the guardian loop in a goroutine.
func StartGuardian(ctx context.Context, cfg GuardianConfig) (*Guardian, error) {
	if cfg.NodeName == "" {
		return nil, fmt.Errorf("guardian requires NodeName")
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 5 * time.Minute
	}
	if cfg.RestartBackoff <= 0 {
		cfg.RestartBackoff = 10 * time.Minute
	}
	if cfg.MinBrokenFor <= 0 {
		cfg.MinBrokenFor = 30 * time.Second
	}
	if cfg.OptInLabelKey == "" {
		cfg.OptInLabelKey = "simplyblock.io/auto-restart-on-pathloss"
	}
	if cfg.OptInLabelValue == "" {
		cfg.OptInLabelValue = "true"
	}

	rc, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("guardian in-cluster config: %w", err)
	}
	cs, err := kubernetes.NewForConfig(rc)
	if err != nil {
		return nil, fmt.Errorf("guardian clientset: %w", err)
	}

	g := &Guardian{
		cfg:                cfg,
		cs:                 cs,
		lvols:              map[string]*LvolState{},
		lastRestart:        map[string]time.Time{},
		clusterWasInactive: map[string]bool{},
	}

	klog.Infof("Guardian started node=%s poll=%s backoff=%s minBrokenFor=%s dryRun=%v",
		cfg.NodeName, cfg.PollInterval, cfg.RestartBackoff, cfg.MinBrokenFor, cfg.DryRun)

	g.loadState()

	go g.loop(ctx)
	return g, nil
}

// RegisterPublish records that a volume (identified by NQN) is published to a pod via targetPath.
// Call this from NodePublishVolume.
func (g *Guardian) RegisterPublish(nqn string, targetPath string) {
	clusterID, lvolID := getLvolIDFromNQN(nqn)
	podUID := podUIDFromTargetPath(targetPath)
	if lvolID == "" || podUID == "" || clusterID == "" {
		return
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	st, ok := g.lvols[lvolID]
	if !ok || st == nil {
		st = &LvolState{PodUIDs: map[string]struct{}{}}
		g.lvols[lvolID] = st
	}
	if st.PodUIDs == nil {
		st.PodUIDs = map[string]struct{}{}
	}
	st.PodUIDs[podUID] = struct{}{}
	st.ClusterID = clusterID

	if _, exists := g.clusterWasInactive[clusterID]; !exists {
		g.clusterWasInactive[clusterID] = true
	}

	g.persistLocked()
}

// RegisterUnpublish removes mapping. Call from NodeUnpublishVolume.
func (g *Guardian) RegisterUnpublishByTargetPath(targetPath string) {
	podUID := podUIDFromTargetPath(targetPath)
	if podUID == "" {
		return
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	for lvolID, st := range g.lvols {
		if st == nil || st.PodUIDs == nil {
			continue
		}
		delete(st.PodUIDs, podUID)

		// If no pods remain, drop the lvol entry entirely (and its BrokenAt).
		if len(st.PodUIDs) == 0 {
			delete(g.lvols, lvolID)
		}
	}

	g.persistLocked()
}

// MarkBrokenLvol marks lvol broken at time.Now() (first time only).
// Call this when you *know* both paths are gone / device removed.
func (g *Guardian) MarkBrokenLvol(lvolID string) {
	if lvolID == "" {
		return
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	st, ok := g.lvols[lvolID]
	if !ok || st == nil {
		klog.Warningf("Guardian: MarkBrokenLvol(%s) ignored: unknown lvol (not published yet?)", lvolID)
		return
	}

	if st.ClusterID == "" {
		klog.Warningf("Guardian: MarkBrokenLvol(%s) ignored: clusterID unknown (not published yet?)", lvolID)
		return
	}

	if st.BrokenAt.IsZero() {
		st.BrokenAt = time.Now().UTC()
		klog.Warningf("Guardian marked lvol broken: cluster=%s lvol=%s", st.ClusterID, lvolID)
	}

	if _, ok := g.clusterWasInactive[st.ClusterID]; !ok {
		g.clusterWasInactive[st.ClusterID] = true
	}

	g.persistLocked()
}

func (g *Guardian) loop(ctx context.Context) {
	t := time.NewTicker(g.cfg.PollInterval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			klog.Infof("Guardian stopping: %v", ctx.Err())
			return
		case <-t.C:
			g.tick(ctx)
		}
	}
}

func (g *Guardian) tick(ctx context.Context) {
	secretFile := FromEnv("SPDKCSI_SECRET", "/etc/spdkcsi-secret/secret.json")
	var clusters ClustersInfo
	if err := ParseJSONFile(secretFile, &clusters); err != nil {
		klog.Errorf("Guardian: parse clusters secret failed: %v", err)
		return
	}
	if len(clusters.Clusters) == 0 {
		return
	}

	// Snapshot current state under lock.
	g.mu.Lock()
	lvolBrokenAt := make(map[string]time.Time, len(g.lvols))
	lvolPods := make(map[string][]string, len(g.lvols))
	lvolCluster := make(map[string]string, len(g.lvols))
	clusterWasInactive := make(map[string]bool, len(g.clusterWasInactive))

	for lvolID, st := range g.lvols {
		if st == nil {
			continue
		}
		lvolCluster[lvolID] = st.ClusterID
		if !st.BrokenAt.IsZero() {
			lvolBrokenAt[lvolID] = st.BrokenAt
		}
		for podUID := range st.PodUIDs {
			lvolPods[lvolID] = append(lvolPods[lvolID], podUID)
		}
	}
	for cid, v := range g.clusterWasInactive {
		clusterWasInactive[cid] = v
	}
	g.mu.Unlock()

	justBecameActive := map[string]bool{} // clusterID -> true
	for _, c := range clusters.Clusters {
		cid := c.ClusterID
		if cid == "" {
			continue
		}

		active, realStatus, err := g.isClusterActiveByID(cid)
		if err != nil {
			klog.Warningf("Guardian: cluster status check failed cluster=%s err=%v (treating as inactive)", cid, err)
			active = false
			realStatus = "unknown"
		}

		wasInactive := clusterWasInactive[cid]
		if !active {
			clusterWasInactive[cid] = true
			continue
		}

		if wasInactive {
			justBecameActive[cid] = true
			klog.Warningf("Guardian: cluster=%s transitioned to %s; will evaluate pod restarts", cid, realStatus)
		}
		clusterWasInactive[cid] = false
	}

	// Persist cluster transition updates back.
	g.mu.Lock()
	for cid, v := range clusterWasInactive {
		g.clusterWasInactive[cid] = v
	}
	g.mu.Unlock()

	if len(justBecameActive) == 0 {
		return
	}

	now := time.Now().UTC()
	actionableByCluster := map[string][]string{} // clusterID -> []lvolID
	for lvolID, ts := range lvolBrokenAt {
		if now.Sub(ts) < g.cfg.MinBrokenFor {
			continue
		}
		cid := lvolCluster[lvolID]
		if cid == "" {
			continue
		}
		if !justBecameActive[cid] {
			continue
		}
		actionableByCluster[cid] = append(actionableByCluster[cid], lvolID)
	}

	if len(actionableByCluster) == 0 {
		return
	}

	klog.Warningf("Guardian: clusters back active=%v; evaluating restarts", keysBoolMap(justBecameActive))

	pods, err := g.listRunningPodsOnNode(ctx, g.cfg.NodeName)
	if err != nil {
		klog.Errorf("Guardian: list pods failed: %v", err)
		return
	}
	uidToPod := map[string]v1.Pod{}
	for _, p := range pods.Items {
		uidToPod[string(p.UID)] = p
	}

	restarted := 0

	for cid, lvolIDs := range actionableByCluster {
		klog.Warningf("Guardian: cluster %s active; attempting restarts for broken lvols=%v", cid, lvolIDs)

		for _, lvolID := range lvolIDs {
			klog.Warningf("Guardian debug: lvol=%s podUIDs=%v", lvolID, lvolPods[lvolID])

			for _, podUID := range lvolPods[lvolID] {
				pod, ok := uidToPod[podUID]
				if !ok {
					continue
				}

				if pod.Labels[g.cfg.OptInLabelKey] != g.cfg.OptInLabelValue {
					continue
				}

				// if pod.Labels[g.cfg.OptOutLabelKey] == g.cfg.OptOutLabelValue {
				// 	continue
				// }

				if !controllerManaged(&pod) {
					continue
				}
				if last, ok := g.getLastRestart(podUID); ok && time.Since(last) < g.cfg.RestartBackoff {
					continue
				}

				klog.Warningf("Guardian: restarting pod %s/%s (uid=%s) due to broken lvol=%s cluster=%s",
					pod.Namespace, pod.Name, podUID, lvolID, cid)

				if !g.cfg.DryRun {
					err := g.cs.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{
						GracePeriodSeconds: &g.cfg.GraceSeconds,
					})
					if err != nil && !apierrors.IsNotFound(err) {
						klog.Errorf("Guardian: delete pod %s/%s failed: %v", pod.Namespace, pod.Name, err)
						continue
					}
				}

				g.setLastRestart(podUID)
				restarted++
			}

			g.mu.Lock()
			if st := g.lvols[lvolID]; st != nil {
				st.BrokenAt = time.Time{}
			}
			g.mu.Unlock()
		}
	}

	if restarted > 0 {
		klog.Infof("Guardian: restart cycle complete. restarted=%d", restarted)
	}

	g.mu.Lock()
	g.persistLocked()
	g.mu.Unlock()
}

func (g *Guardian) isClusterActiveByID(clusterID string) (ok bool, realStatus string, err error) {
	node, err := NewsimplyBlockClient(clusterID)
	if err != nil {
		return false, "", err
	}

	resp, err := node.Client.CallSBCLI("GET", "/cluster", nil)
	if err != nil {
		return false, "", err
	}

	var status []ClusterStatus
	data, _ := json.Marshal(resp)
	if err := json.Unmarshal(data, &status); err != nil {
		return false, "", err
	}
	if len(status) == 0 {
		return false, "", fmt.Errorf("empty cluster status response")
	}

	realStatus = strings.ToLower(strings.TrimSpace(status[0].Status))
	ok = (realStatus == "active" || realStatus == "degraded")
	return ok, realStatus, nil
}

func (g *Guardian) listRunningPodsOnNode(ctx context.Context, nodeName string) (*v1.PodList, error) {
	selector := fields.AndSelectors(
		fields.OneTermEqualSelector("spec.nodeName", nodeName),
		fields.OneTermEqualSelector("status.phase", string(v1.PodRunning)),
	).String()

	return g.cs.CoreV1().Pods("").List(ctx, metav1.ListOptions{FieldSelector: selector})
}

func controllerManaged(pod *v1.Pod) bool {
	for _, r := range pod.OwnerReferences {
		if r.Controller != nil && *r.Controller {
			return true
		}
	}
	return false
}

func (g *Guardian) getLastRestart(podUID string) (time.Time, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	t, ok := g.lastRestart[podUID]
	return t, ok
}

func (g *Guardian) setLastRestart(podUID string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.lastRestart[podUID] = time.Now()
}

// Extract pod UID from kubelet targetPath.
// Example: /var/lib/kubelet/pods/<uid>/volumes/kubernetes.io~csi/.../mount
func podUIDFromTargetPath(p string) string {
	const marker = "/pods/"
	i := strings.Index(p, marker)
	if i < 0 {
		return ""
	}
	rest := p[i+len(marker):]
	j := strings.Index(rest, "/")
	if j < 0 {
		return ""
	}
	return rest[:j]
}

func (g *Guardian) persistLocked() {
	if g.cfg.StatePath == "" {
		return
	}

	st := guardianState{
		Lvols:              map[string]persistedLvolState{},
		LastRestart:        map[string]time.Time{},
		ClusterWasInactive: map[string]bool{},
	}

	for lvolID, lvs := range g.lvols {
		if lvs == nil {
			continue
		}
		pls := persistedLvolState{
			ClusterID: lvs.ClusterID,
			BrokenAt:  lvs.BrokenAt,
		}
		for uid := range lvs.PodUIDs {
			pls.PodUIDs = append(pls.PodUIDs, uid)
		}
		st.Lvols[lvolID] = pls
	}

	for uid, t := range g.lastRestart {
		st.LastRestart[uid] = t
	}
	for cid, v := range g.clusterWasInactive {
		st.ClusterWasInactive[cid] = v
	}

	b, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		klog.Errorf("Guardian: marshal state: %v", err)
		return
	}

	dir := filepath.Dir(g.cfg.StatePath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		klog.Errorf("Guardian: mkdir state dir %s: %v", dir, err)
		return
	}
	if err := os.WriteFile(g.cfg.StatePath, b, 0o600); err != nil {
		klog.Errorf("Guardian: write state: %v", err)
	}
}

func keysBoolMap(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k, v := range m {
		if v {
			out = append(out, k)
		}
	}
	return out
}
