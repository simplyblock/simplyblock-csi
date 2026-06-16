// Property-based tests for the Guardian's restart-eligibility logic.
//
// Two categories:
//   1. Invariant tests  – properties derived from the spec that MUST hold.
//   2. Spec tests       – properties derived from design comments; FAIL on current code.
//
// Run with:
//   go test ./pkg/util/ -run TestPBT -v
//   go test ./pkg/util/ -run TestSpec -v

package util

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"pgregory.net/rapid"
)

// ─── helpers ─────────────────────────────────────────────────────────────────

func testGuardianCfg() GuardianConfig {
	return GuardianConfig{
		NodeName:        "test-node",
		PollInterval:    5 * time.Minute,
		RestartBackoff:  10 * time.Minute,
		MinBrokenFor:    30 * time.Second,
		OptInLabelKey:   "simplyblock.io/auto-restart-on-pathloss",
		OptInLabelValue: "true",
		CSIDriverName:   "csi.simplyblock.io",
		StatePath:       "", // disable file I/O
	}
}

// evalRestart is the pure predicate distilled from tick()'s restart-qualification
// logic. It mirrors the ACTUAL implementation so we can verify its invariants.
//
//	guardian.go tick() lines 392-404 (lvolBrokenAt filter) + line 446 (backoff check)
func evalRestart(
	brokenAt time.Time,
	clusterActive bool,
	lastRestartAt time.Time, // zero = never restarted
	now time.Time,
	cfg GuardianConfig,
) bool {
	if brokenAt.IsZero() {
		return false
	}
	if !clusterActive {
		return false
	}
	if now.Sub(brokenAt) < cfg.MinBrokenFor {
		return false
	}
	if !lastRestartAt.IsZero() && now.Sub(lastRestartAt) < cfg.RestartBackoff {
		return false
	}
	return true
}

// ─── Invariant 1: inactive cluster must never trigger a restart ───────────────
//
// Safety property: no matter how long an lvol has been broken, if the cluster
// is not currently active the pod must not be restarted.

func TestPBT_InactiveCluster_NeverRestarts(t *testing.T) {
	cfg := testGuardianCfg()
	rapid.Check(t, func(t *rapid.T) {
		now := time.Now()
		hoursAgo := rapid.Int64Range(0, 168).Draw(t, "hours_ago")
		brokenAt := now.Add(-time.Duration(hoursAgo) * time.Hour)

		if evalRestart(brokenAt, false /*inactive*/, time.Time{}, now, cfg) {
			t.Fatalf("safety violation: restart triggered on inactive cluster\n"+
				"brokenAt=%v (%dh ago)", brokenAt, hoursAgo)
		}
	})
}

// ─── Invariant 2: unbroken lvol (zero BrokenAt) must never restart ───────────

func TestPBT_UnbrokenLvol_NeverRestarts(t *testing.T) {
	cfg := testGuardianCfg()
	rapid.Check(t, func(t *rapid.T) {
		clusterActive := rapid.Bool().Draw(t, "cluster_active")

		if evalRestart(time.Time{} /*zero*/, clusterActive, time.Time{}, time.Now(), cfg) {
			t.Fatalf("safety violation: restart triggered on unbroken lvol (zero BrokenAt), "+
				"clusterActive=%v", clusterActive)
		}
	})
}

// ─── Invariant 3: MinBrokenFor threshold must be enforced ────────────────────
//
// A broken lvol that has not been broken for long enough must not trigger a restart
// regardless of cluster state.

func TestPBT_MinBrokenFor_ThresholdGate(t *testing.T) {
	cfg := testGuardianCfg()
	rapid.Check(t, func(t *rapid.T) {
		now := time.Now()
		// Pick any duration strictly less than MinBrokenFor.
		ns := rapid.Int64Range(0, int64(cfg.MinBrokenFor)-1).Draw(t, "ns_since_broken")
		brokenAt := now.Add(-time.Duration(ns))

		if evalRestart(brokenAt, true, time.Time{}, now, cfg) {
			t.Fatalf("threshold violation: restart triggered before MinBrokenFor elapsed\n"+
				"elapsed=%v < minBrokenFor=%v", time.Duration(ns), cfg.MinBrokenFor)
		}
	})
}

// ─── Invariant 4: RestartBackoff must prevent immediate re-restart ────────────

func TestPBT_RestartBackoff_Gate(t *testing.T) {
	cfg := testGuardianCfg()
	rapid.Check(t, func(t *rapid.T) {
		now := time.Now()
		brokenAt := now.Add(-2 * cfg.MinBrokenFor) // broken long enough

		// lastRestart within the backoff window.
		ns := rapid.Int64Range(0, int64(cfg.RestartBackoff)-1).Draw(t, "ns_since_restart")
		lastRestartAt := now.Add(-time.Duration(ns))

		if evalRestart(brokenAt, true, lastRestartAt, now, cfg) {
			t.Fatalf("backoff violation: restart triggered during backoff window\n"+
				"lastRestart=%v ago, backoff=%v", time.Duration(ns), cfg.RestartBackoff)
		}
	})
}

// ─── Invariant 5: liveness – all conditions satisfied → must restart ──────────

func TestPBT_Liveness_EligibleMustRestart(t *testing.T) {
	cfg := testGuardianCfg()
	rapid.Check(t, func(t *rapid.T) {
		now := time.Now()
		extra := rapid.Int64Range(1, int64(time.Hour)).Draw(t, "extra_past_threshold")
		brokenAt := now.Add(-cfg.MinBrokenFor - time.Duration(extra))

		if !evalRestart(brokenAt, true, time.Time{}, now, cfg) {
			t.Fatalf("liveness violation: eligible lvol not restarted\n"+
				"brokenFor=%v >= minBrokenFor=%v, cluster active, no prior restart",
				now.Sub(brokenAt), cfg.MinBrokenFor)
		}
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Spec test 1 – no restart without a cluster recovery transition
//
// Source: guardian.go design comment:
//   "restarts pods ONLY after cluster becomes active again"
//
// "Becomes active again" implies a prior inactive period. A cluster that was
// continuously active never "becomes active again", so no restart should fire
// even if an lvol has been broken long enough.
// ─────────────────────────────────────────────────────────────────────────────

func TestSpec_NoRestartWithoutClusterRecovery(t *testing.T) {
	cfg := testGuardianCfg()
	rapid.Check(t, func(t *rapid.T) {
		now := time.Now()
		extra := rapid.Int64Range(1, int64(time.Hour)).Draw(t, "extra")
		brokenAt := now.Add(-cfg.MinBrokenFor - time.Duration(extra))

		// Draw whether the cluster was previously inactive.
		// The spec only permits a restart when wasInactive=true (recovery transition).
		wasInactive := rapid.Bool().Draw(t, "was_inactive")

		result := evalRestart(brokenAt, true /*cluster active now*/, time.Time{}, now, cfg)

		// Property: restart is only valid after an inactive→active transition.
		// When wasInactive=false (cluster was always up), result must be false.
		if !wasInactive && result {
			t.Fatalf("spec violation: restart triggered without a cluster recovery transition\n"+
				"spec: 'restarts pods ONLY after cluster becomes active again'\n"+
				"wasInactive=%v  brokenFor=%v  minBrokenFor=%v",
				wasInactive, now.Sub(brokenAt), cfg.MinBrokenFor)
		}
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Spec test 2 – RestartBackoff must protect a pod across all its lvols
//
// Source: RestartBackoff field in GuardianConfig:
//   "minimum time between pod restarts"
//
// A pod that uses N broken lvols could be processed N times in a single tick
// (once per lvol). The backoff record must survive each removal so the pod
// cannot be restarted more than once per tick regardless of how many lvols it owns.
// ─────────────────────────────────────────────────────────────────────────────

func TestSpec_RestartBackoff_HoldsAcrossAllLvols(t *testing.T) {
	cfg := testGuardianCfg()

	rapid.Check(t, func(t *rapid.T) {
		g := &Guardian{
			cfg:                cfg,
			lvols:              map[string]*LvolState{},
			lastRestart:        map[string]time.Time{},
			clusterWasInactive: map[string]bool{},
		}

		const podUID = "pod-A"
		const clusterID = "cluster-1"

		// rapid picks how many lvols pod A is attached to (always ≥ 2).
		n := rapid.IntRange(2, 5).Draw(t, "n_lvols")
		lvolIDs := make([]string, n)
		for i := range n {
			lvolIDs[i] = fmt.Sprintf("lvol-%d", i)
			g.lvols[lvolIDs[i]] = &LvolState{
				PodUIDs:   map[string]struct{}{podUID: {}},
				ClusterID: clusterID,
			}
		}

		// tick() restarts pod A and records the backoff.
		g.setLastRestart(podUID)

		// Simulate tick() iterating over broken lvols and cleaning up each one.
		// After every removal, if pod A still appears in any remaining lvol the
		// backoff MUST be present — otherwise the pod can be deleted again in
		// the same tick, violating RestartBackoff.
		for _, lvolID := range lvolIDs {
			g.mu.Lock()
			g.removePodFromLvolLocked(lvolID, podUID)
			g.mu.Unlock()

			remaining := 0
			for _, id := range lvolIDs {
				if st, ok := g.lvols[id]; ok {
					if _, in := st.PodUIDs[podUID]; in {
						remaining++
					}
				}
			}

			_, backoffPresent := g.getLastRestart(podUID)
			if remaining > 0 && !backoffPresent {
				t.Fatalf("spec violation: RestartBackoff lost for %q after processing %q\n"+
					"pod still tracked by %d lvol(s) — backoff must prevent re-restart in same tick\n"+
					"spec: RestartBackoff=%v must hold for the entire tick duration",
					podUID, lvolID, remaining, cfg.RestartBackoff)
			}
		}
	})
}

// ─── getLvolIDFromNQN: result must be both-empty or both-non-empty ─────────────
//
// Callers (RegisterPublish, MarkBrokenLvol) guard with:
//   if lvolID == "" || clusterID == "" { return }
// so a half-empty result is safe at the call site, but the asymmetry is a
// latent footgun for future callers that check only one field.
//
// Trigger: any NQN ending with ":lvol:" produces clusterID="<X>", lvolID="".
// e.g. "nqn.2016-06.io.spdk:cnodeABC:lvol:" → clusterID="cnodeABC", lvolID=""
//
// rapid.String() is too random to hit ":lvol:" reliably, so the generator is
// biased: 50% of inputs are constructed with an explicit ":lvol:" separator.

func TestPBT_GetLvolIDFromNQN_BothOrNeither(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		var nqn string
		if rapid.Bool().Draw(t, "inject_delimiter") {
			// Construct NQNs with an explicit :lvol: separator so rapid
			// exercises the parsing branch. Empty suffix triggers the bug.
			prefix := rapid.String().Draw(t, "prefix")
			suffix := rapid.String().Draw(t, "suffix")
			nqn = prefix + ":lvol:" + suffix
		} else {
			nqn = rapid.String().Draw(t, "nqn")
		}

		clusterID, lvolID := getLvolIDFromNQN(nqn)

		bothEmpty := clusterID == "" && lvolID == ""
		bothNonEmpty := clusterID != "" && lvolID != ""
		if !bothEmpty && !bothNonEmpty {
			t.Fatalf("BUG – inconsistent result: clusterID=%q lvolID=%q\n"+
				"nqn=%q\n"+
				"Two triggers:\n"+
				"  - NQN starting with ':lvol:' → clusterID empty, lvolID non-empty\n"+
				"  - NQN ending with ':lvol:'   → clusterID non-empty, lvolID empty\n"+
				"Fix: return (\"\",\"\") whenever either extracted segment is empty.",
				clusterID, lvolID, nqn)
		}
	})
}

// ─── podUIDFromTargetPath: canonical kubelet paths must round-trip ────────────

func TestPBT_PodUIDFromTargetPath_RoundTrip(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Generate a plausible pod UID (UUID-like, no slashes or the word "pods").
		uid := rapid.StringMatching(`[a-z0-9][a-z0-9-]{4,34}[a-z0-9]`).Draw(t, "uid")
		if strings.Contains(uid, "/") || strings.Contains(uid, "pods") {
			return // skip; rapid will generate another sample
		}
		path := "/var/lib/kubelet/pods/" + uid + "/volumes/kubernetes.io~csi/pvc-x/mount"
		if got := podUIDFromTargetPath(path); got != uid {
			t.Fatalf("round-trip failed\npath=%q\nexpected=%q\ngot=%q", path, uid, got)
		}
	})
}

// ─── podUIDFromTargetPath: result must never contain a slash ──────────────────

func TestPBT_PodUIDFromTargetPath_ResultHasNoSlash(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		path := rapid.String().Draw(t, "path")
		got := podUIDFromTargetPath(path)
		if strings.Contains(got, "/") {
			t.Fatalf("result contains '/': path=%q got=%q", path, got)
		}
	})
}

// ─── podUIDFromTargetPath: edge case – path ends at UID with no trailing slash ─
//
// kubelet never emits this shape, but the function silently returns "".
// Documented here so future changes don't accidentally break the invariant.

func TestEdge_PodUIDFromTargetPath_NoTrailingSlash(t *testing.T) {
	path := "/var/lib/kubelet/pods/abc-123" // no further path segments
	got := podUIDFromTargetPath(path)
	// Current behaviour: strings.Index("abc-123", "/") == -1 → returns "".
	// This is safe because RegisterPublish rejects empty podUID.
	if got != "" {
		t.Fatalf("expected \"\" for truncated path, got %q", got)
	}
}
