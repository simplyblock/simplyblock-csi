package util

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
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
	NodeName        string
	PollInterval    time.Duration
	RestartBackoff  time.Duration
	GraceSeconds    int64
	OptInLabelKey   string
	OptInLabelValue string
	DryRun          bool

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
		NodeName:        nodeName,
		PollInterval:    5 * time.Second,
		RestartBackoff:  10 * time.Minute,
		GraceSeconds:    0,
		OptInLabelKey:   "simplyblock.io/auto-restart-on-pathloss",
		OptInLabelValue: "true",
		DryRun:          false,
		MinBrokenFor:    30 * time.Second,
		StatePath:       "/var/run/simplyblock/guardian/state.json",
	}
}

// Guardian tracks which pod uses which lvol and restarts affected pods
// ONLY after cluster becomes active again.
type Guardian struct {
	cfg GuardianConfig

	cs *kubernetes.Clientset

	mu sync.Mutex

	// lvolID -> set(podUID)
	lvolPods map[string]map[string]struct{}

	// lvolID -> first time observed "broken"
	lvolBrokenAt map[string]time.Time

	// podUID -> last restart time
	lastRestart map[string]time.Time

	// cluster transition state
	clusterWasInactive bool
}

// StartGuardian starts the guardian loop in a goroutine.
func StartGuardian(ctx context.Context, cfg GuardianConfig) (*Guardian, error) {
	if cfg.NodeName == "" {
		return nil, fmt.Errorf("guardian requires NodeName")
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 5 * time.Second
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
		lvolPods:           map[string]map[string]struct{}{},
		lvolBrokenAt:       map[string]time.Time{},
		lastRestart:        map[string]time.Time{},
		clusterWasInactive: true,
	}

	klog.Infof("Guardian started node=%s poll=%s backoff=%s minBrokenFor=%s dryRun=%v",
		cfg.NodeName, cfg.PollInterval, cfg.RestartBackoff, cfg.MinBrokenFor, cfg.DryRun)

	go g.loop(ctx)
	return g, nil
}

// RegisterPublish records that a volume (identified by NQN) is published to a pod via targetPath.
// Call this from NodePublishVolume.
func (g *Guardian) RegisterPublish(nqn string, targetPath string) {
	lvolID := lvolIDFromNQN(nqn)
	podUID := podUIDFromTargetPath(targetPath)
	if lvolID == "" || podUID == "" {
		return
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	if g.lvolPods[lvolID] == nil {
		g.lvolPods[lvolID] = map[string]struct{}{}
	}
	g.lvolPods[lvolID][podUID] = struct{}{}

	g.persistLocked()
}

// RegisterUnpublish removes mapping. Call from NodeUnpublishVolume.
func (g *Guardian) RegisterUnpublish(nqn string, targetPath string) {
	lvolID := lvolIDFromNQN(nqn)
	podUID := podUIDFromTargetPath(targetPath)
	if lvolID == "" || podUID == "" {
		return
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	if set := g.lvolPods[lvolID]; set != nil {
		delete(set, podUID)
		if len(set) == 0 {
			delete(g.lvolPods, lvolID)
		}
	}

	g.persistLocked()
}

func (g *Guardian) RegisterUnpublishByTargetPath(targetPath string) {
	podUID := podUIDFromTargetPath(targetPath)
	if podUID == "" {
		return
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	for lvolID, set := range g.lvolPods {
		delete(set, podUID)
		if len(set) == 0 {
			delete(g.lvolPods, lvolID)
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

	if _, exists := g.lvolBrokenAt[lvolID]; !exists {
		g.lvolBrokenAt[lvolID] = time.Now().UTC()
		klog.Warningf("Guardian marked lvol broken: %s", lvolID)
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
	active := g.isClusterActive()

	if !active {
		g.clusterWasInactive = true
		return
	}

	if !g.clusterWasInactive {
		return
	}
	g.clusterWasInactive = false

	g.mu.Lock()
	broken := make(map[string]time.Time, len(g.lvolBrokenAt))
	for lvolID, ts := range g.lvolBrokenAt {
		broken[lvolID] = ts
	}
	lvolPods := make(map[string][]string, len(g.lvolPods))
	for lvolID, set := range g.lvolPods {
		for podUID := range set {
			lvolPods[lvolID] = append(lvolPods[lvolID], podUID)
		}
	}
	g.mu.Unlock()

	if len(broken) == 0 {
		return
	}

	now := time.Now().UTC()
	actionable := []string{}
	for lvolID, ts := range broken {
		if now.Sub(ts) >= g.cfg.MinBrokenFor {
			actionable = append(actionable, lvolID)
		}
	}
	if len(actionable) == 0 {
		return
	}

	klog.Warningf("Guardian: cluster active; attempting restarts for broken lvols=%v", actionable)

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

	for _, lvolID := range actionable {
		podUIDs := lvolPods[lvolID]
		for _, podUID := range podUIDs {
			pod, ok := uidToPod[podUID]
			if !ok {
				continue
			}

			if pod.Labels[g.cfg.OptInLabelKey] != g.cfg.OptInLabelValue {
				continue
			}
			if !controllerManaged(&pod) {
				continue
			}
			if last, ok := g.getLastRestart(podUID); ok && time.Since(last) < g.cfg.RestartBackoff {
				continue
			}

			klog.Warningf("Guardian: restarting pod %s/%s (uid=%s) due to broken lvol=%s",
				pod.Namespace, pod.Name, podUID, lvolID)

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
		delete(g.lvolBrokenAt, lvolID)
		g.mu.Unlock()
	}

	if restarted > 0 {
		klog.Infof("Guardian: restart cycle complete. restarted=%d", restarted)
	}
}

func (g *Guardian) isClusterActive() bool {
	secretFile := FromEnv("SPDKCSI_SECRET", "/etc/spdkcsi-secret/secret.json")

	var clusters ClustersInfo
	if err := ParseJSONFile(secretFile, &clusters); err != nil {
		klog.Errorf("Guardian: parse clusters secret failed: %v", err)
		return false
	}

	for _, c := range clusters.Clusters {
		if c.ClusterID == "" {
			continue
		}

		node, err := NewsimplyBlockClient(c.ClusterID)
		if err != nil {
			continue
		}

		resp, err := node.Client.CallSBCLI("GET", "/cluster", nil)
		if err != nil {
			klog.Errorf("Guardian: failed to fetch cluster status for %s: %v", c.ClusterID, err)
			return false
		}

		var status []ClusterStatus
		data, _ := json.Marshal(resp)
		if err := json.Unmarshal(data, &status); err != nil {
			klog.Errorf("Guardian: failed to unmarshal cluster status for %s: %v", c.ClusterID, err)
			return false
		}

		if len(status) == 0 {
			return false
		}

		return strings.ToLower(status[0].Status) == "active"
	}

	return false
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

func lvolIDFromNQN(nqn string) string {
	parts := strings.Split(nqn, ":lvol:")
	if len(parts) != 2 {
		return ""
	}
	return strings.TrimSpace(parts[1])
}

func (g *Guardian) persistLocked() {
	if g.cfg.StatePath == "" {
		return
	}

	type state struct {
		LvolPods     map[string][]string  `json:"lvolPods"`
		LvolBrokenAt map[string]time.Time `json:"lvolBrokenAt"`
	}

	out := state{
		LvolPods:     map[string][]string{},
		LvolBrokenAt: map[string]time.Time{},
	}

	for lvolID, set := range g.lvolPods {
		for podUID := range set {
			out.LvolPods[lvolID] = append(out.LvolPods[lvolID], podUID)
		}
	}
	for lvolID, t := range g.lvolBrokenAt {
		out.LvolBrokenAt[lvolID] = t
	}

	b, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		klog.Errorf("Guardian: marshal state: %v", err)
		return
	}

	_ = os.MkdirAll(strings.TrimSuffix(g.cfg.StatePath, "/state.json"), 0o755)
	if err := os.WriteFile(g.cfg.StatePath, b, 0o600); err != nil {
		klog.Errorf("Guardian: write state: %v", err)
	}
}
