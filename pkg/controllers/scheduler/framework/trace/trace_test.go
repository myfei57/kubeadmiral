/*
Copyright 2023 The KubeAdmiral Authors.

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

package trace

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	fedcorev1a1 "github.com/kubewharf/kubeadmiral/pkg/apis/core/v1alpha1"
)

func testBudget() Budget {
	return Budget{
		MaxCandidates:     2,
		MaxLabelEntries:   2,
		MaxReasonsPerEval: 2,
		MaxReasonBytes:    16,
		MaxOutputBytes:    4096,
	}
}

func newTestObserver(budget Budget) *Observer {
	return NewObserver(TraceMeta{
		FederatedObjectUID:        types.UID("obj-uid"),
		FederatedObjectGeneration: 7,
		FederatedObjectKey:        "default/nginx",
		PolicyDigest:              "digest",
		Clusters: []ClusterSnapshot{
			{Name: "c1", UID: types.UID("uid-1")},
		},
	}, &budget)
}

func TestNilObserverIsNoOp(t *testing.T) {
	var ob *Observer
	// None of these must panic.
	ob.Begin()
	ob.Finish(OutcomeFailed, StageFilter)
	ob.MarkTruncated("x")
	ob.RecordFilterEval("c1", PluginEval{Plugin: "p"})
	ob.RecordScorePlugin(PluginScoreTrace{})
	ob.RecordSelectPlugin(SelectPluginTrace{})
	ob.RecordReplicasSkipped("skip")
	ob.RecordReplicaEvidence(nil)
	if got := ob.Snapshot(); got != nil {
		t.Fatalf("nil observer snapshot should be nil, got %#v", got)
	}
	if got := ObserverFromContext(context.Background()); got != nil {
		t.Fatalf("expected nil observer from plain context")
	}
}

func TestContextRoundTrip(t *testing.T) {
	ob := newTestObserver(testBudget())
	ctx := WithObserver(context.Background(), ob)
	if got := ObserverFromContext(ctx); got != ob {
		t.Fatalf("observer lost through context")
	}
	if WithObserver(ctx, nil) != ctx {
		t.Fatalf("nil observer should return the original context")
	}
}

func TestFilterShortCircuitRecordsNotEvaluated(t *testing.T) {
	ob := newTestObserver(testBudget())
	ob.RecordFilterPluginOrder([]string{"A", "B", "C"})
	ob.RecordFilterEval("c1", PluginEval{Plugin: "A", State: Evaluated, Code: CodeSuccess})
	ob.RecordFilterEval("c1", PluginEval{
		Plugin: "B", State: Evaluated, Code: CodeUnschedulable, Reasons: []string{"no capacity"},
	})
	ob.RecordFilterEval("c1", PluginEval{Plugin: "C", State: NotEvaluated})
	ob.RecordFilterOutcome([]string{}, []string{"c1"})

	snap := ob.Snapshot()
	if snap.Filter == nil {
		t.Fatalf("filter stage missing")
	}
	if got := len(snap.Filter.Plugins); got != 3 {
		t.Fatalf("expected 3 configured plugins, got %d", got)
	}
	entry := snap.Filter.Clusters[0]
	if entry.RejectedBy != "B" {
		t.Fatalf("expected rejected by B, got %q", entry.RejectedBy)
	}
	if entry.Plugins[2].State != NotEvaluated {
		t.Fatalf("plugin C must be NotEvaluated after short-circuit")
	}
	if snap.Outcome != "" {
		t.Fatalf("outcome should only be set by Finish")
	}
	ob.Finish(OutcomeInfeasible, StageFilter)
	if got := ob.Snapshot().Outcome; got != OutcomeInfeasible {
		t.Fatalf("expected Infeasible outcome, got %q", got)
	}
}

func TestReasonsAreSanitizedAndBounded(t *testing.T) {
	ob := newTestObserver(testBudget())
	ob.RecordFilterEval("c1", PluginEval{
		Plugin: "P",
		State:  Evaluated,
		Code:   CodeError,
		Reasons: []string{
			"db password leaked here",        // sensitive content -> redacted
			"DB_TOKEN=abcdef",                // sensitive env -> redacted
			"third reason should be dropped", // exceeds MaxReasonsPerEval
		},
	})
	longReason := "this is a very long reason that exceeds the small per-fragment byte budget"
	ob.RecordFilterEval("c2", PluginEval{
		Plugin:  "Q",
		State:   Evaluated,
		Code:    CodeUnschedulable,
		Reasons: []string{longReason},
	})

	snap := ob.Snapshot()
	evals := snap.Filter.Clusters
	var pEval, qEval *PluginEval
	for i := range evals {
		for j := range evals[i].Plugins {
			switch evals[i].Plugins[j].Plugin {
			case "P":
				pEval = &evals[i].Plugins[j]
			case "Q":
				qEval = &evals[i].Plugins[j]
			}
		}
	}
	if pEval == nil || qEval == nil {
		t.Fatalf("missing eval entries")
	}
	if len(pEval.Reasons) != 2 {
		t.Fatalf("expected 2 bounded reasons, got %d: %v", len(pEval.Reasons), pEval.Reasons)
	}
	for _, reason := range pEval.Reasons {
		if reason != redactionMarker {
			t.Fatalf("sensitive reason must be redacted, got %q", reason)
		}
		if strings.Contains(reason, "abcdef") || strings.Contains(reason, "password") {
			t.Fatalf("secret material leaked into trace: %q", reason)
		}
	}
	if !strings.HasSuffix(qEval.Reasons[0], "...<truncated>") {
		t.Fatalf("long reason must be truncated, got %q", qEval.Reasons[0])
	}
	if len(qEval.Reasons[0]) > testBudget().MaxReasonBytes+len("...<truncated>") {
		t.Fatalf("reason fragment exceeds byte budget")
	}
}

func TestCandidateBudgetMarksTruncated(t *testing.T) {
	ob := newTestObserver(testBudget()) // MaxCandidates = 2
	for _, cluster := range []string{"c1", "c2", "c3"} {
		ob.RecordFilterEval(cluster, PluginEval{Plugin: "P", State: Evaluated, Code: CodeSuccess})
	}
	snap := ob.Snapshot()
	if !snap.Truncated {
		t.Fatalf("trace must be marked truncated when candidate budget is exceeded")
	}
	if len(snap.Filter.Clusters) != 2 {
		t.Fatalf("expected only 2 candidate entries, got %d", len(snap.Filter.Clusters))
	}
	found := false
	for _, reason := range snap.TruncationReasons {
		if strings.Contains(reason, "candidateBudgetExceeded") {
			found = true
		}
	}
	if !found {
		t.Fatalf("truncation reason must identify the exhausted budget, got %v", snap.TruncationReasons)
	}
}

func TestLabelSnapshotIsBoundedAndRedacted(t *testing.T) {
	clusters := []*fedcorev1a1.FederatedCluster{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "c1",
				UID:  types.UID("uid-1"),
				Labels: map[string]string{
					"zone":        "a",
					"env":         "prod",
					"extra":       "x",
					"login-token": "secret-value",
				},
			},
		},
	}
	snapshots := BuildClusterSnapshots(clusters, Budget{MaxCandidates: 4, MaxLabelEntries: 4})
	if len(snapshots) != 1 {
		t.Fatalf("expected 1 snapshot")
	}
	if len(snapshots[0].Labels) != 4 {
		t.Fatalf("label entries must be retained within budget, got %d", len(snapshots[0].Labels))
	}
	if got := snapshots[0].Labels["login-token"]; got != redactionMarker {
		t.Fatalf("sensitive label value must be redacted, got %q", got)
	}

	bounded := BuildClusterSnapshots(clusters, Budget{MaxCandidates: 4, MaxLabelEntries: 2})
	if len(bounded[0].Labels) != 2 {
		t.Fatalf("label entries must be bounded, got %d", len(bounded[0].Labels))
	}
}

func TestSnapshotIsFrozenDeepCopy(t *testing.T) {
	labels := map[string]string{"zone": "a"}
	ob := NewObserver(TraceMeta{
		FederatedObjectUID: types.UID("uid"),
		Clusters: []ClusterSnapshot{
			{Name: "c1", UID: types.UID("uid-1"), Labels: labels},
		},
	}, nil)
	ob.RecordFilterEval("c1", PluginEval{Plugin: "P", State: Evaluated, Code: CodeSuccess})

	first := ob.Snapshot()
	// Mutate caller-side data after the snapshot was taken.
	labels["zone"] = "b"
	labels["injected"] = "true"
	// Mutate the returned snapshot.
	first.Meta.Clusters[0].Labels["zone"] = "tampered"
	first.Filter.Clusters[0].Plugins[0].Plugin = "tampered"

	second := ob.Snapshot()
	if second.Meta.Clusters[0].Labels["zone"] != "a" {
		t.Fatalf("observer retained mutable caller data: %v", second.Meta.Clusters[0].Labels)
	}
	if _, ok := second.Meta.Clusters[0].Labels["injected"]; ok {
		t.Fatalf("observer must copy label maps at construction")
	}
	if second.Filter.Clusters[0].Plugins[0].Plugin != "P" {
		t.Fatalf("snapshot must be independent of earlier snapshot mutations")
	}
}

func TestOutputByteBudgetTruncatesExplanationsOnly(t *testing.T) {
	budget := testBudget()
	budget.MaxOutputBytes = 128 // impossibly small for any real trace
	ob := newTestObserver(budget)
	ob.RecordFilterEval("c1", PluginEval{
		Plugin: "P", State: Evaluated, Code: CodeUnschedulable, Reasons: []string{"explanatory reason"},
	})
	ob.Finish(OutcomeInfeasible, StageFilter)

	snap := ob.Snapshot()
	if !snap.Truncated {
		t.Fatalf("byte budget overflow must be marked")
	}
	// Structural decision data survives; only explanations are stripped.
	if snap.Filter == nil || len(snap.Filter.Clusters) != 1 {
		t.Fatalf("structural filter data must not be truncated")
	}
	if snap.Filter.Clusters[0].Plugins[0].Plugin != "P" {
		t.Fatalf("plugin identity is structural data and must survive")
	}
	if len(snap.Filter.Clusters[0].Plugins[0].Reasons) != 0 {
		t.Fatalf("reason explanations must be stripped when over byte budget")
	}
	if snap.Outcome != OutcomeInfeasible {
		t.Fatalf("outcome must survive truncation")
	}
}

func TestDigestStructStableAndDistinct(t *testing.T) {
	type policy struct {
		Placement string `json:"placement"`
	}
	d1 := DigestStruct(policy{Placement: "a"})
	d2 := DigestStruct(policy{Placement: "a"})
	d3 := DigestStruct(policy{Placement: "b"})
	if d1 == "" || d1 != d2 {
		t.Fatalf("digest must be deterministic and non-empty")
	}
	if d1 == d3 {
		t.Fatalf("different inputs must produce different digests")
	}
	if len(d1) != 64 {
		t.Fatalf("expected sha256 hex digest, got length %d", len(d1))
	}
}

func TestOpaqueMarker(t *testing.T) {
	opaque := opaquePluginStub{name: "webhook"}
	if !IsOpaque(&opaque) {
		t.Fatalf("plugin implementing OpaqueDecisionPlugin must be detected")
	}
	if IsOpaque(nil) {
		t.Fatalf("nil plugin must not be opaque")
	}
	plain := plainPluginStub{name: "builtin"}
	if IsOpaque(&plain) {
		t.Fatalf("plain plugin must not be opaque")
	}
}

type opaquePluginStub struct{ name string }

func (o *opaquePluginStub) Name() string              { return o.name }
func (o *opaquePluginStub) SchedulingDecisionOpaque() {}

type plainPluginStub struct{ name string }

func (p *plainPluginStub) Name() string { return p.name }

func TestSnapshotIsJSONSerializable(t *testing.T) {
	ob := newTestObserver(testBudget())
	ob.Begin()
	ob.RecordFilterPluginOrder([]string{"P"})
	ob.RecordFilterEval("c1", PluginEval{Plugin: "P", State: Evaluated, Code: CodeSuccess})
	ob.RecordFilterOutcome([]string{"c1"}, nil)
	ob.Finish(OutcomeSucceeded, "")

	encoded, err := json.Marshal(ob.Snapshot())
	if err != nil {
		t.Fatalf("trace must serialize to JSON: %v", err)
	}
	if !strings.Contains(string(encoded), `"outcome":"Succeeded"`) {
		t.Fatalf("serialized trace missing outcome: %s", string(encoded))
	}
}
