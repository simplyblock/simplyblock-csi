---------------------------- MODULE Guardian ----------------------------
(*
  TLA+ specification of the Guardian's pod-restart state machine.
  Repository: github.com/simplyblock/simplyblock-csi
  Source:     pkg/util/guardian.go

  The Guardian periodically polls cluster health and restarts pods that own
  broken lvols — but ONLY after a cluster recovers from an outage.

  Design comment from guardian.go (lines 86-88):
    "Guardian tracks which pod uses which lvol and restarts affected pods
     ONLY after cluster becomes active again."

  This module encodes two behaviours for comparison:

    Spec          -- ACTUAL implementation (tick() as written; contains Bug 1)
    SpecIntended  -- INTENDED implementation (wasInactive gate applied)

  ------------------------------------------------------------------
  Bug 1: missing justBecameActive gate
    tick() computes `justBecameActive` (line 350) but gates the restart
    action on `activeNow[cid]` (line 401) rather than `justBecameActive`.
    Effect: pods are restarted whenever an lvol has been broken long
    enough AND the cluster is currently active — no outage-then-recovery
    cycle is required.

    TLC finds: NoSpuriousCandidates FAILS under Spec,
                              PASSES under SpecIntended.

  TLC counterexample (MinBrokenFor=2, MaxClock=8, one cluster C1):
    1. RegisterPublish(L1,P1,C1)  -- seenClusters={C1}, wasInactive={C1}
    2. RegisterPublish(L2,P2,C1)  -- C1 already seen; no change
    3. MarkBroken(L1)             -- lvolBrokenAt[L1]=clock, C1 already seen
    4. AdvanceClock x2
    5. GuardianTick               -- restarts P1; wasInactive reset to {}
                                     seenClusters stays {C1}
    6. MarkBroken(L2)             -- C1 in seenClusters → wasInactive unchanged
    7. AdvanceClock x2
    8. [L2 eligible, C1 active, wasInactive={} → JustBecameActive=FALSE
       → NoSpuriousCandidates VIOLATED]
  ------------------------------------------------------------------

  Bug 2: RestartBackoff bypass in multi-lvol pods
    removePodFromLvolLocked deletes lastRestart[podUID] when the pod
    vacates its last slot on ONE lvol, even if still on others.  tick()
    can then re-encounter the pod on a second lvol in the same pass.
    Modelling this requires an atomic vs. non-atomic tick distinction.
    Captured by the PBT suite (TestSpec_RestartBackoff_HoldsAcrossAllLvols)
    and left as an exercise here.
  ------------------------------------------------------------------

  Run TLC with Guardian.cfg.  The counterexample for Bug 1 appears in
  under a minute with the small constants in that file.
*)

EXTENDS Naturals, FiniteSets

CONSTANTS
    LvolIDs,        \* finite set of lvol identifiers
    PodUIDs,        \* finite set of pod UIDs
    ClusterIDs,     \* finite set of cluster IDs
    MinBrokenFor,   \* ticks lvol must stay broken before action is eligible
    RestartBackoff, \* minimum ticks between restarts of the same pod
    MaxClock        \* upper clock bound for finite model checking

ASSUME MinBrokenFor   \in (Nat \ {0})
ASSUME RestartBackoff \in (Nat \ {0})
ASSUME MaxClock       \in Nat
ASSUME MaxClock > MinBrokenFor + RestartBackoff

\* ---------------------------------------------------------------------------
\* STATE VARIABLES
\* ---------------------------------------------------------------------------
(*
  clusterWasInactive in Go is a map[string]bool whose KEY EXISTENCE matters:

    key absent            -> "never seen" (treated as true by RegisterPublish
                             and MarkBrokenLvol due to the !exists guard)
    key present, val TRUE -> cluster was inactive at some point since last tick
    key present, val FALSE-> cluster was active on the most recent tick

  We model this with two separate sets:

    wasInactive    -- SUBSET ClusterIDs: clusters where map value = TRUE
    seenClusters   -- SUBSET ClusterIDs: clusters whose map key EXISTS

  Invariant maintained: wasInactive ⊆ seenClusters.
*)

VARIABLES
    lvolCluster,    \* LvolID -> ClusterID | "none"
    lvolPods,       \* LvolID -> SUBSET PodUIDs
    lvolBrokenAt,   \* LvolID -> Nat  (clock tick when broken; 0 = healthy)
    clusterStatus,  \* ClusterID -> {"active","inactive"}
    wasInactive,    \* SUBSET ClusterIDs
    seenClusters,   \* SUBSET ClusterIDs
    lastRestartAt,  \* PodUID -> Nat  (last restart clock tick; 0 = never)
    clock           \* Nat

vars == <<lvolCluster, lvolPods, lvolBrokenAt,
          clusterStatus, wasInactive, seenClusters,
          lastRestartAt, clock>>

\* ---------------------------------------------------------------------------
\* TYPE INVARIANT
\* ---------------------------------------------------------------------------

TypeOK ==
    /\ lvolCluster   \in [LvolIDs    -> ClusterIDs \cup {"none"}]
    /\ lvolPods      \in [LvolIDs    -> SUBSET PodUIDs]
    /\ lvolBrokenAt  \in [LvolIDs    -> Nat]
    /\ clusterStatus \in [ClusterIDs -> {"active", "inactive"}]
    /\ wasInactive   \subseteq ClusterIDs
    /\ seenClusters  \subseteq ClusterIDs
    /\ wasInactive   \subseteq seenClusters
    /\ lastRestartAt \in [PodUIDs    -> Nat]
    /\ clock         \in Nat

\* ---------------------------------------------------------------------------
\* HELPERS
\* ---------------------------------------------------------------------------

RegisteredLvols == {l \in LvolIDs : lvolCluster[l] # "none"}

\* Lvol l has been broken long enough to qualify for action.
EligibleBroken(l) ==
    /\ lvolBrokenAt[l] > 0
    /\ clock - lvolBrokenAt[l] >= MinBrokenFor

\* Pod p is outside its restart backoff window.
BackoffExpired(p) ==
    lastRestartAt[p] = 0 \/ clock - lastRestartAt[p] >= RestartBackoff

\* Cluster c has just recovered: it was previously flagged inactive AND is now active.
\* This is what the design intent requires — not merely "is active now".
JustBecameActive(c) ==
    /\ clusterStatus[c] = "active"
    /\ c \in wasInactive

(*
  SeenUpdate(c) — models the Go idiom:
    if _, exists := g.clusterWasInactive[c]; !exists {
        g.clusterWasInactive[c] = true
    }

  Only adds c to seenClusters/wasInactive when the key did NOT previously exist.
  After a tick has written the key (c ∈ seenClusters), neither RegisterPublish
  nor MarkBrokenLvol will set wasInactive[c] = true again.
*)
SeenUpdate(c, wi, sc) ==
    IF c \notin sc
    THEN <<wi \cup {c}, sc \cup {c}>>
    ELSE <<wi, sc>>

\* ---------------------------------------------------------------------------
\* INITIAL STATE
\* ---------------------------------------------------------------------------

Init ==
    /\ lvolCluster   = [l \in LvolIDs    |-> "none"]
    /\ lvolPods      = [l \in LvolIDs    |-> {}]
    /\ lvolBrokenAt  = [l \in LvolIDs    |-> 0]
    /\ clusterStatus = [c \in ClusterIDs |-> "active"]
    /\ wasInactive   = {}
    /\ seenClusters  = {}
    /\ lastRestartAt = [p \in PodUIDs    |-> 0]
    /\ clock         = 1

\* ---------------------------------------------------------------------------
\* ENVIRONMENT ACTIONS
\* ---------------------------------------------------------------------------

(*
  RegisterPublish(l, p, c) — pod p begins using lvol l in cluster c.
  Mirrors guardian.go RegisterPublish (lines 218-240).

  The !exists guard:
    if _, exists := g.clusterWasInactive[c]; !exists {
        g.clusterWasInactive[c] = true
    }
  adds c to seenClusters only when not yet present.
*)
RegisterPublish(l, p, c) ==
    /\ lvolCluster[l] = "none"
    /\ lvolCluster'  = [lvolCluster EXCEPT ![l] = c]
    /\ lvolPods'     = [lvolPods    EXCEPT ![l] = {p}]
    /\ lvolBrokenAt' = lvolBrokenAt
    /\ LET upd == SeenUpdate(c, wasInactive, seenClusters)
       IN  /\ wasInactive'  = upd[1]
           /\ seenClusters' = upd[2]
    /\ UNCHANGED <<clusterStatus, lastRestartAt, clock>>

(*
  Unpublish(l, p) — pod p stops using lvol l.
  Mirrors guardian.go UnregisterPublish (NodeUnpublishVolume path).
*)
Unpublish(l, p) ==
    /\ p \in lvolPods[l]
    /\ LET remaining == lvolPods[l] \ {p}
       IN  /\ lvolPods'    = [lvolPods    EXCEPT ![l] = remaining]
           /\ lvolCluster' = [lvolCluster EXCEPT ![l] =
                                  IF remaining = {} THEN "none" ELSE lvolCluster[l]]
           /\ lvolBrokenAt'= [lvolBrokenAt EXCEPT ![l] =
                                  IF remaining = {} THEN 0 ELSE lvolBrokenAt[l]]
    /\ UNCHANGED <<clusterStatus, wasInactive, seenClusters, lastRestartAt, clock>>

(*
  MarkBroken(l) — storage path loss marks lvol l broken (first call only).
  Mirrors guardian.go MarkBrokenLvol (lines 283-297).

  Same !exists guard as RegisterPublish.
*)
MarkBroken(l) ==
    /\ lvolCluster[l] # "none"
    /\ lvolPods[l] # {}
    /\ lvolBrokenAt[l] = 0
    /\ lvolBrokenAt' = [lvolBrokenAt EXCEPT ![l] = clock]
    /\ LET c   == lvolCluster[l]
           upd == SeenUpdate(c, wasInactive, seenClusters)
       IN  /\ wasInactive'  = upd[1]
           /\ seenClusters' = upd[2]
    /\ UNCHANGED <<lvolCluster, lvolPods, clusterStatus, lastRestartAt, clock>>

\* Cluster c suffers an outage.
ClusterGoesInactive(c) ==
    /\ clusterStatus[c] = "active"
    /\ clusterStatus' = [clusterStatus EXCEPT ![c] = "inactive"]
    /\ wasInactive'   = wasInactive  \cup {c}
    /\ seenClusters'  = seenClusters \cup {c}
    /\ UNCHANGED <<lvolCluster, lvolPods, lvolBrokenAt, lastRestartAt, clock>>

\* Cluster c recovers.
ClusterBecomesActive(c) ==
    /\ clusterStatus[c] = "inactive"
    /\ clusterStatus' = [clusterStatus EXCEPT ![c] = "active"]
    /\ UNCHANGED <<lvolCluster, lvolPods, lvolBrokenAt,
                   wasInactive, seenClusters, lastRestartAt, clock>>

\* Time passes between Guardian poll intervals.
AdvanceClock ==
    /\ clock < MaxClock
    /\ clock' = clock + 1
    /\ UNCHANGED <<lvolCluster, lvolPods, lvolBrokenAt,
                   clusterStatus, wasInactive, seenClusters, lastRestartAt>>

\* ---------------------------------------------------------------------------
\* GUARDIAN TICK — ACTUAL IMPLEMENTATION (Bug 1 present)
\*
\* Mirrors tick() in guardian.go.
\*
\* BUG: the guard is `clusterStatus[c] = "active"` (activeNow), not
\*      JustBecameActive(c). tick() computes justBecameActive (line 350)
\*      but the actionableByCluster map (line 401) uses activeNow instead.
\* ---------------------------------------------------------------------------

GuardianTick ==
    /\ clock < MaxClock
    /\ LET
           \* Candidates: broken long enough, backoff expired, cluster active.
           \* ACTUAL gate: cluster is active (not "just became active").
           candidates == {<<l, p>> \in RegisteredLvols \X PodUIDs :
                              /\ p \in lvolPods[l]
                              /\ EligibleBroken(l)
                              /\ clusterStatus[lvolCluster[l]] = "active"  \* BUG 1
                              /\ BackoffExpired(p)}
           restarted  == {p \in PodUIDs : \E l \in LvolIDs : <<l, p>> \in candidates}
           \* Remove restarted pods from their lvols.
           newPods    == [l \in LvolIDs |->
                              lvolPods[l] \ {p \in PodUIDs : <<l, p>> \in candidates}]
       IN
       /\ candidates # {}
       /\ lvolPods'     = newPods
       /\ lastRestartAt' = [p \in PodUIDs |->
                                IF p \in restarted THEN clock ELSE lastRestartAt[p]]
       \* Clean up lvol entries where all pods have been removed.
       /\ lvolCluster'  = [l \in LvolIDs |->
                               IF newPods[l] = {} THEN "none" ELSE lvolCluster[l]]
       /\ lvolBrokenAt' = [l \in LvolIDs |->
                               IF newPods[l] = {} THEN 0 ELSE lvolBrokenAt[l]]
       \* tick() line 377: for active clusters, set clusterWasInactive[cid] = false
       \* (key now exists in the Go map, value = false → removed from wasInactive set).
       /\ wasInactive'  = wasInactive  \ {c \in ClusterIDs : clusterStatus[c] = "active"}
       /\ seenClusters' = seenClusters \cup {c \in ClusterIDs : clusterStatus[c] = "active"}
    /\ clock' = clock + 1
    /\ UNCHANGED clusterStatus

\* ---------------------------------------------------------------------------
\* GUARDIAN TICK — INTENDED IMPLEMENTATION (Bug 1 fixed)
\*
\* Gates restart on JustBecameActive(c): cluster was previously inactive
\* AND is now active.  Idle-active clusters are skipped.
\* ---------------------------------------------------------------------------

GuardianTickIntended ==
    /\ clock < MaxClock
    /\ LET
           candidates == {<<l, p>> \in RegisteredLvols \X PodUIDs :
                              /\ p \in lvolPods[l]
                              /\ EligibleBroken(l)
                              /\ JustBecameActive(lvolCluster[l])  \* FIX
                              /\ BackoffExpired(p)}
           restarted  == {p \in PodUIDs : \E l \in LvolIDs : <<l, p>> \in candidates}
           newPods    == [l \in LvolIDs |->
                              lvolPods[l] \ {p \in PodUIDs : <<l, p>> \in candidates}]
       IN
       /\ candidates # {}
       /\ lvolPods'     = newPods
       /\ lastRestartAt' = [p \in PodUIDs |->
                                IF p \in restarted THEN clock ELSE lastRestartAt[p]]
       /\ lvolCluster'  = [l \in LvolIDs |->
                               IF newPods[l] = {} THEN "none" ELSE lvolCluster[l]]
       /\ lvolBrokenAt' = [l \in LvolIDs |->
                               IF newPods[l] = {} THEN 0 ELSE lvolBrokenAt[l]]
       /\ wasInactive'  = wasInactive  \ {c \in ClusterIDs : clusterStatus[c] = "active"}
       /\ seenClusters' = seenClusters \cup {c \in ClusterIDs : clusterStatus[c] = "active"}
    /\ clock' = clock + 1
    /\ UNCHANGED clusterStatus

\* ---------------------------------------------------------------------------
\* NEXT-STATE RELATIONS
\* ---------------------------------------------------------------------------

Next ==
    \/ \E l \in LvolIDs, p \in PodUIDs, c \in ClusterIDs : RegisterPublish(l, p, c)
    \/ \E l \in LvolIDs, p \in PodUIDs                   : Unpublish(l, p)
    \/ \E l \in LvolIDs                                   : MarkBroken(l)
    \/ \E c \in ClusterIDs : ClusterGoesInactive(c)
    \/ \E c \in ClusterIDs : ClusterBecomesActive(c)
    \/ AdvanceClock
    \/ GuardianTick

NextIntended ==
    \/ \E l \in LvolIDs, p \in PodUIDs, c \in ClusterIDs : RegisterPublish(l, p, c)
    \/ \E l \in LvolIDs, p \in PodUIDs                   : Unpublish(l, p)
    \/ \E l \in LvolIDs                                   : MarkBroken(l)
    \/ \E c \in ClusterIDs : ClusterGoesInactive(c)
    \/ \E c \in ClusterIDs : ClusterBecomesActive(c)
    \/ AdvanceClock
    \/ GuardianTickIntended

\* ---------------------------------------------------------------------------
\* SAFETY INVARIANTS
\* ---------------------------------------------------------------------------

(*
  S1 — Inactive cluster: a pod whose cluster is currently down must not
  be restarted this tick.  Both tick variants honour this (they both
  check clusterStatus = "active"), so S1 holds under both Spec and SpecIntended.
*)
NoRestartOnInactiveCluster ==
    \A l \in RegisteredLvols, p \in lvolPods[l] :
        clusterStatus[lvolCluster[l]] = "inactive" =>
        lastRestartAt[p] # clock

(*
  S2 — MinBrokenFor threshold enforced: no restart before an lvol has been
  broken for at least MinBrokenFor ticks.
*)
MinBrokenForRespected ==
    \A l \in RegisteredLvols, p \in lvolPods[l] :
        (lvolBrokenAt[l] > 0 /\ clock - lvolBrokenAt[l] < MinBrokenFor) =>
        lastRestartAt[p] # clock

(*
  S3 — No spurious restart candidates (Bug 1 invariant):

  Whenever an eligible restart candidate exists (lvol broken long enough,
  backoff expired, cluster currently active), the cluster must have JUST
  recovered (JustBecameActive).  If this is violated, GuardianTick will
  restart the pod without any prior outage.

  FAILS  under Spec         (activeNow gate, not JustBecameActive).
  PASSES under SpecIntended (JustBecameActive gate enforced).

  TLC counterexample: see the trace in the module header comment.
*)
NoSpuriousCandidates ==
    \A l \in RegisteredLvols, p \in lvolPods[l] :
        ( EligibleBroken(l)
        /\ BackoffExpired(p)
        /\ clusterStatus[lvolCluster[l]] = "active" ) =>
        JustBecameActive(lvolCluster[l])

\* ---------------------------------------------------------------------------
\* LIVENESS PROPERTY
\* ---------------------------------------------------------------------------

(*
  L1 — After cluster recovery, pods on eligible lvols are eventually restarted.
  Requires weak fairness on GuardianTickIntended; check with FairSpecIntended.
*)
EventualRestartAfterRecovery ==
    \A l \in LvolIDs, p \in PodUIDs :
        ( lvolBrokenAt[l] > 0
        /\ p \in lvolPods[l]
        /\ JustBecameActive(lvolCluster[l]) ) ~>
        ( p \notin lvolPods[l] )

\* ---------------------------------------------------------------------------
\* SPECIFICATIONS
\* ---------------------------------------------------------------------------

\* Actual implementation — contains Bug 1.
Spec             == Init /\ [][Next]_vars

\* Actual with weak fairness (needed if checking liveness properties).
FairSpec         == Spec /\ WF_vars(GuardianTick)

\* Intended / corrected implementation.
SpecIntended     == Init /\ [][NextIntended]_vars

\* Intended with fairness — use this to verify EventualRestartAfterRecovery.
FairSpecIntended == SpecIntended /\ WF_vars(GuardianTickIntended)

============================================================================
