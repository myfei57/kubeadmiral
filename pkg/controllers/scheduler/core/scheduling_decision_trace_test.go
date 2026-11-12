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

package core

import (
	"context"
	"reflect"
	"sync/atomic"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/utils/pointer"

	fedcore "github.com/kubewharf/kubeadmiral/pkg/apis/core"
	fedcorev1a1 "github.com/kubewharf/kubeadmiral/pkg/apis/core/v1alpha1"
	"github.com/kubewharf/kubeadmiral/pkg/controllers/scheduler/framework"
	"github.com/kubewharf/kubeadmiral/pkg/controllers/scheduler/framework/plugins/maxcluster"
	"github.com/kubewharf/kubeadmiral/pkg/controllers/scheduler/framework/plugins/names"
	"github.com/kubewharf/kubeadmiral/pkg/controllers/scheduler/framework/plugins/preferencebinpack"
	"github.com/kubewharf/kubeadmiral/pkg/controllers/scheduler/framework/runtime"
	schedtrace "github.com/kubewharf/kubeadmiral/pkg/controllers/scheduler/framework/trace"
	"github.com/kubewharf/kubeadmiral/pkg/stats"
)

// ---------- counting fakes ----------

type countingFilterPlugin struct {
	name         string
	calls        int64
	failClusters sets.Set[string]
}

var _ framework.FilterPlugin = &countingFilterPlugin{}

func (p *countingFilterPlugin) Name() string { return p.name }

func (p *countingFilterPlugin) Filter(
	_ context.Context,
	_ *framework.SchedulingUnit,
	cluster *fedcorev1a1.FederatedCluster,
) *framework.Result {
	atomic.AddInt64(&p.calls, 1)
	if p.failClusters.Has(cluster.Name) {
		return framework.NewResult(framework.Unschedulable, "configured to reject "+cluster.Name)
	}
	return framework.NewResult(framework.Success)
}

type opaqueFilterPlugin struct {
	name  string
	calls int64
}

var (
	_ framework.FilterPlugin          = &opaqueFilterPlugin{}
	_ schedtrace.OpaqueDecisionPlugin = &opaqueFilterPlugin{}
)

func (p *opaqueFilterPlugin) Name() string              { return p.name }
func (p *opaqueFilterPlugin) SchedulingDecisionOpaque() {}
func (p *opaqueFilterPlugin) Filter(
	_ context.Context,
	_ *framework.SchedulingUnit,
	_ *fedcorev1a1.FederatedCluster,
) *framework.Result {
	atomic.AddInt64(&p.calls, 1)
	// An external webhook-style failure: generic message, no internal reason.
	return framework.NewResult(framework.Error, "webhook request failed")
}

type countingScorePlugin struct {
	name           string
	calls          int64
	normalizeCalls int64
	scores         map[string]int64
	withNormalize  bool
	fail           bool
}

var _ framework.ScorePlugin = &countingScorePlugin{}

func (p *countingScorePlugin) Name() string { return p.name }

func (p *countingScorePlugin) Score(
	_ context.Context,
	_ *framework.SchedulingUnit,
	cluster *fedcorev1a1.FederatedCluster,
) (int64, *framework.Result) {
	atomic.AddInt64(&p.calls, 1)
	if p.fail {
		return 0, framework.NewResult(framework.Error, "score backend unavailable")
	}
	return p.scores[cluster.Name], framework.NewResult(framework.Success)
}

func (p *countingScorePlugin) ScoreExtensions() framework.ScoreExtensions {
	if !p.withNormalize {
		return nil
	}
	return p
}

func (p *countingScorePlugin) NormalizeScore(
	_ context.Context,
	scores framework.ClusterScoreList,
) *framework.Result {
	atomic.AddInt64(&p.normalizeCalls, 1)
	return framework.DefaultNormalizeScore(framework.MaxClusterScore, false, scores)
}

// ---------- helpers ----------

func traceTestClusters() []*fedcorev1a1.FederatedCluster {
	return []*fedcorev1a1.FederatedCluster{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "cluster1",
				UID:    types.UID("uid-cluster1"),
				Labels: map[string]string{"zone": "a"},
			},
			Status: fedcorev1a1.FederatedClusterStatus{
				Conditions: []fedcorev1a1.ClusterCondition{
					{Type: fedcorev1a1.ClusterJoined, Status: corev1.ConditionTrue},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "cluster2",
				UID:    types.UID("uid-cluster2"),
				Labels: map[string]string{"zone": "b"},
			},
			Status: fedcorev1a1.FederatedClusterStatus{
				Conditions: []fedcorev1a1.ClusterCondition{
					{Type: fedcorev1a1.ClusterJoined, Status: corev1.ConditionTrue},
				},
			},
		},
	}
}

func newTraceTestFramework(
	t *testing.T,
	registry runtime.Registry,
	enabledPlugins *fedcore.EnabledPlugins,
) framework.Framework {
	t.Helper()
	metrics := stats.NewMock("test", "kubeadmiral_controller_manager", false)
	fwk, err := runtime.NewFramework(registry, nil, enabledPlugins, "test-profile", metrics)
	if err != nil {
		t.Fatalf("unexpected error creating framework: %v", err)
	}
	return fwk
}

func runWithObserver(
	t *testing.T,
	fwk framework.Framework,
	su framework.SchedulingUnit,
	clusters []*fedcorev1a1.FederatedCluster,
) (ScheduleResult, *schedtrace.SchedulingDecisionTrace) {
	t.Helper()
	observer := schedtrace.NewObserver(schedtrace.TraceMeta{
		FederatedObjectUID:        types.UID("fed-object-uid"),
		FederatedObjectGeneration: 3,
		FederatedObjectKey:        "default/deploy",
		PolicyDigest:              schedtrace.DigestStruct(su.SchedulingMode),
		SchedulingProfile:         "test-profile",
		Clusters:                  schedtrace.BuildClusterSnapshots(clusters, schedtrace.DefaultBudget()),
	}, nil)
	ctx := schedtrace.WithObserver(context.Background(), observer)
	result, err := NewSchedulerAlgorithm().Schedule(ctx, fwk, su, clusters)
	if err != nil {
		t.Fatalf("unexpected scheduling error: %v", err)
	}
	return result, observer.Snapshot()
}

func mustFilterEntry(t *testing.T, decisionTrace *schedtrace.SchedulingDecisionTrace, cluster string) *schedtrace.ClusterFilterTrace {
	t.Helper()
	for i := range decisionTrace.Filter.Clusters {
		if decisionTrace.Filter.Clusters[i].Cluster == cluster {
			return &decisionTrace.Filter.Clusters[i]
		}
	}
	t.Fatalf("filter entry for cluster %q not found", cluster)
	return nil
}

func mustScorePluginTrace(
	t *testing.T,
	decisionTrace *schedtrace.SchedulingDecisionTrace,
	plugin string,
) *schedtrace.PluginScoreTrace {
	t.Helper()
	for i := range decisionTrace.Score.Plugins {
		if decisionTrace.Score.Plugins[i].Plugin == plugin {
			return &decisionTrace.Score.Plugins[i]
		}
	}
	t.Fatalf("score plugin trace %q not found", plugin)
	return nil
}

func evalByPlugin(evals []schedtrace.PluginEval, plugin string) *schedtrace.PluginEval {
	for i := range evals {
		if evals[i].Plugin == plugin {
			return &evals[i]
		}
	}
	return nil
}

func scoreEvalByCluster(
	evals []schedtrace.ClusterScoreEval,
	cluster string,
) *schedtrace.ClusterScoreEval {
	for i := range evals {
		if evals[i].Cluster == cluster {
			return &evals[i]
		}
	}
	return nil
}

// ---------- tests ----------

func TestTraceFilterShortCircuitMarksNotEvaluated(t *testing.T) {
	filterA := &countingFilterPlugin{name: "FilterA"}
	filterB := &countingFilterPlugin{name: "FilterB", failClusters: sets.New("cluster1")}
	filterC := &countingFilterPlugin{name: "FilterC"}
	registry := runtime.Registry{
		"FilterA": func(_ framework.Handle) (framework.Plugin, error) { return filterA, nil },
		"FilterB": func(_ framework.Handle) (framework.Plugin, error) { return filterB, nil },
		"FilterC": func(_ framework.Handle) (framework.Plugin, error) { return filterC, nil },
	}
	fwk := newTraceTestFramework(t, registry, &fedcore.EnabledPlugins{
		FilterPlugins: []string{"FilterA", "FilterB", "FilterC"},
	})

	su := framework.SchedulingUnit{SchedulingMode: fedcorev1a1.SchedulingModeDuplicate}
	result, decisionTrace := runWithObserver(t, fwk, su, traceTestClusters())

	// cluster1 fails at FilterB, FilterC must never be invoked for it.
	cluster1 := mustFilterEntry(t, decisionTrace, "cluster1")
	evalA := evalByPlugin(cluster1.Plugins, "FilterA")
	if evalA == nil || evalA.State != schedtrace.Evaluated || evalA.Code != schedtrace.CodeSuccess {
		t.Fatalf("FilterA on cluster1 mis-recorded: %#v", evalA)
	}
	evalB := evalByPlugin(cluster1.Plugins, "FilterB")
	if evalB == nil || evalB.State != schedtrace.Evaluated ||
		evalB.Code != schedtrace.CodeUnschedulable {
		t.Fatalf("FilterB on cluster1 mis-recorded: %#v", evalB)
	}
	if got := evalByPlugin(cluster1.Plugins, "FilterC"); got == nil || got.State != schedtrace.NotEvaluated {
		t.Fatalf("FilterC must be NotEvaluated after short-circuit: %#v", got)
	}
	if cluster1.RejectedBy != "FilterB" {
		t.Fatalf("expected RejectedBy FilterB, got %q", cluster1.RejectedBy)
	}

	// cluster2 passes the whole chain.
	cluster2 := mustFilterEntry(t, decisionTrace, "cluster2")
	for _, eval := range cluster2.Plugins {
		if eval.State != schedtrace.Evaluated {
			t.Fatalf("all plugins must run on the feasible cluster2, got %#v", eval)
		}
	}

	if decisionTrace.Outcome != schedtrace.OutcomeSucceeded {
		t.Fatalf("expected Succeeded outcome, got %q", decisionTrace.Outcome)
	}
	if !reflect.DeepEqual(result.ClusterSet(), sets.New("cluster2")) {
		t.Fatalf("only cluster2 should be scheduled, got %v", result.SuggestedClusters)
	}

	// Call-count proof: the short-circuited plugin runs exactly once (cluster2 only).
	if got := atomic.LoadInt64(&filterC.calls); got != 1 {
		t.Fatalf("FilterC expected exactly 1 invocation after short-circuit, got %d", got)
	}
	if got := atomic.LoadInt64(&filterB.calls); got != 2 {
		t.Fatalf("FilterB expected 2 invocations, got %d", got)
	}
}

func TestTraceSameScoreTieAndSelectElimination(t *testing.T) {
	scorePlugin := &countingScorePlugin{
		name:   "EqualScore",
		scores: map[string]int64{"cluster1": 10, "cluster2": 10},
	}
	registry := runtime.Registry{
		"EqualScore": func(_ framework.Handle) (framework.Plugin, error) { return scorePlugin, nil },
		"MaxCluster": maxcluster.NewMaxCluster,
	}
	fwk := newTraceTestFramework(t, registry, &fedcore.EnabledPlugins{
		ScorePlugins:  []string{"EqualScore"},
		SelectPlugins: []string{"MaxCluster"},
	})

	su := framework.SchedulingUnit{
		SchedulingMode: fedcorev1a1.SchedulingModeDuplicate,
		MaxClusters:    pointer.Int64(1),
	}
	result, decisionTrace := runWithObserver(t, fwk, su, traceTestClusters())

	if len(result.SuggestedClusters) != 1 {
		t.Fatalf("MaxCluster=1 must leave one cluster, got %v", result.SuggestedClusters)
	}
	if len(decisionTrace.Score.TieGroups) != 1 ||
		!reflect.DeepEqual(decisionTrace.Score.TieGroups[0], []string{"cluster1", "cluster2"}) {
		t.Fatalf("expected one tie group with both clusters, got %#v", decisionTrace.Score.TieGroups)
	}
	for _, aggregated := range decisionTrace.Score.Aggregated {
		if aggregated.TieGroup != 1 || aggregated.TotalScore != 10 {
			t.Fatalf("aggregated score must flag the tie group, got %#v", aggregated)
		}
	}

	if len(decisionTrace.Select.Plugins) != 1 {
		t.Fatalf("expected one select plugin trace")
	}
	selectTrace := decisionTrace.Select.Plugins[0]
	if len(selectTrace.Eliminated) != 1 {
		t.Fatalf("one cluster must be eliminated, got %v", selectTrace.Eliminated)
	}
	if len(selectTrace.OutputOrder) != 1 || len(decisionTrace.Select.FinalOrder) != 1 {
		t.Fatalf("final order must contain the observed winner only")
	}
	// The observed winner is the one actually scheduled: the trace must not
	// invent a tie-break rule, it records the real ordering.
	if selectTrace.OutputOrder[0] != decisionTrace.Select.FinalOrder[0] {
		t.Fatalf("final order must match the observed select output")
	}
	if !sets.New(selectTrace.Eliminated...).Has(selectTrace.Eliminated[0]) {
		t.Fatalf("eliminated cluster identity must be stable")
	}
}

func TestTraceRecordsRawNormalizedAndWeight(t *testing.T) {
	normalizing := &countingScorePlugin{
		name:          "Normalizing",
		scores:        map[string]int64{"cluster1": 50, "cluster2": 100},
		withNormalize: true,
	}
	plain := &countingScorePlugin{
		name:   "Plain",
		scores: map[string]int64{"cluster1": 7, "cluster2": 3},
	}
	registry := runtime.Registry{
		"Normalizing": func(_ framework.Handle) (framework.Plugin, error) { return normalizing, nil },
		"Plain":       func(_ framework.Handle) (framework.Plugin, error) { return plain, nil },
	}
	fwk := newTraceTestFramework(t, registry, &fedcore.EnabledPlugins{
		ScorePlugins: []string{"Normalizing", "Plain"},
	})

	su := framework.SchedulingUnit{SchedulingMode: fedcorev1a1.SchedulingModeDuplicate}
	_, decisionTrace := runWithObserver(t, fwk, su, traceTestClusters())

	normalizingTrace := mustScorePluginTrace(t, decisionTrace, "Normalizing")
	if normalizingTrace.Weight != 1 {
		t.Fatalf("effective plugin weight must be recorded, got %d", normalizingTrace.Weight)
	}
	if normalizingTrace.NormalizeState != schedtrace.Evaluated {
		t.Fatalf("NormalizeScore ran and must be marked Evaluated")
	}
	c1 := scoreEvalByCluster(normalizingTrace.Clusters, "cluster1")
	c2 := scoreEvalByCluster(normalizingTrace.Clusters, "cluster2")
	if c1.RawScore != 50 || c2.RawScore != 100 {
		t.Fatalf("raw scores must be captured before in-place normalization, got %d/%d", c1.RawScore, c2.RawScore)
	}
	if c1.NormalizedScore == nil || *c1.NormalizedScore != 50 {
		t.Fatalf("normalized cluster1 score must be 50, got %#v", c1.NormalizedScore)
	}
	if c2.NormalizedScore == nil || *c2.NormalizedScore != 100 {
		t.Fatalf("normalized cluster2 score must be 100, got %#v", c2.NormalizedScore)
	}

	plainTrace := mustScorePluginTrace(t, decisionTrace, "Plain")
	if plainTrace.NormalizeState != schedtrace.NotEvaluated {
		t.Fatalf("plugin without ScoreExtensions must be NotEvaluated for normalization")
	}
	plainC1 := scoreEvalByCluster(plainTrace.Clusters, "cluster1")
	if plainC1.NormalizedScore != nil {
		t.Fatalf("no normalized score may exist when NormalizeScore did not run")
	}
	if plainC1.RawScore != 7 {
		t.Fatalf("plain raw score must still be recorded")
	}

	// Aggregation must show each plugin's contribution, not just the total.
	for _, aggregated := range decisionTrace.Score.Aggregated {
		if len(aggregated.PluginScores) != 2 {
			t.Fatalf("aggregated explanation must include per-plugin scores, got %#v", aggregated.PluginScores)
		}
	}
}

func TestTraceOpaqueWebhookFailureHasStageAndNoInventedReason(t *testing.T) {
	webhook := &opaqueFilterPlugin{name: "ExternalWebhook"}
	registry := runtime.Registry{
		"ExternalWebhook": func(_ framework.Handle) (framework.Plugin, error) { return webhook, nil },
	}
	fwk := newTraceTestFramework(t, registry, &fedcore.EnabledPlugins{
		FilterPlugins: []string{"ExternalWebhook"},
	})

	observer := schedtrace.NewObserver(schedtrace.TraceMeta{}, nil)
	ctx := schedtrace.WithObserver(context.Background(), observer)
	_, err := NewSchedulerAlgorithm().Schedule(
		ctx, fwk, framework.SchedulingUnit{SchedulingMode: fedcorev1a1.SchedulingModeDuplicate}, traceTestClusters(),
	)
	if err == nil {
		t.Fatalf("expected FitError when every cluster is rejected")
	}
	var fitErr *framework.FitError
	if !asFitError(err, &fitErr) {
		t.Fatalf("expected FitError, got %T: %v", err, err)
	}

	decisionTrace := observer.Snapshot()
	if decisionTrace.Outcome != schedtrace.OutcomeInfeasible ||
		decisionTrace.TerminalStage != schedtrace.StageFilter {
		t.Fatalf("infeasible run must record the Filter terminal stage, got %q/%q",
			decisionTrace.Outcome, decisionTrace.TerminalStage)
	}
	for _, clusterTrace := range decisionTrace.Filter.Clusters {
		for _, eval := range clusterTrace.Plugins {
			if !eval.Opaque {
				t.Fatalf("webhook eval must be marked Opaque: %#v", eval)
			}
			if eval.Code != schedtrace.CodeError {
				t.Fatalf("webhook failure code must be observed verbatim, got %q", eval.Code)
			}
			if len(eval.Reasons) == 0 {
				t.Fatalf("observed webhook error text must be retained")
			}
			// framework.Result surfaces Error reasons through both err and
			// reasons; every fragment must still be the observed text, never
			// an invented cause.
			for _, reason := range eval.Reasons {
				if reason != "webhook request failed" {
					t.Fatalf("trace must not invent causes, got %q", reason)
				}
			}
		}
	}
	// Exactly one invocation per candidate: tracing must never re-run the webhook.
	if got := atomic.LoadInt64(&webhook.calls); got != int64(len(traceTestClusters())) {
		t.Fatalf("webhook must be called once per cluster only, got %d", got)
	}
}

func TestTraceScoreFailureMarksRemainingPluginsNotEvaluated(t *testing.T) {
	failing := &countingScorePlugin{name: "FailingScore", fail: true}
	after := &countingScorePlugin{name: "AfterScore", scores: map[string]int64{"cluster1": 1, "cluster2": 1}}
	registry := runtime.Registry{
		"FailingScore": func(_ framework.Handle) (framework.Plugin, error) { return failing, nil },
		"AfterScore":   func(_ framework.Handle) (framework.Plugin, error) { return after, nil },
	}
	fwk := newTraceTestFramework(t, registry, &fedcore.EnabledPlugins{
		ScorePlugins: []string{"FailingScore", "AfterScore"},
	})

	observer := schedtrace.NewObserver(schedtrace.TraceMeta{}, nil)
	ctx := schedtrace.WithObserver(context.Background(), observer)
	_, err := NewSchedulerAlgorithm().Schedule(
		ctx, fwk, framework.SchedulingUnit{SchedulingMode: fedcorev1a1.SchedulingModeDuplicate}, traceTestClusters(),
	)
	if err == nil {
		t.Fatalf("expected score stage error")
	}
	decisionTrace := observer.Snapshot()
	if decisionTrace.Outcome != schedtrace.OutcomeFailed ||
		decisionTrace.TerminalStage != schedtrace.StageScore {
		t.Fatalf("failed run must pin the Score stage, got %q/%q",
			decisionTrace.Outcome, decisionTrace.TerminalStage)
	}
	afterTrace := mustScorePluginTrace(t, decisionTrace, "AfterScore")
	for _, eval := range afterTrace.Clusters {
		if eval.State != schedtrace.NotEvaluated {
			t.Fatalf("plugins after a failure must be NotEvaluated, got %#v", eval)
		}
	}
	if got := atomic.LoadInt64(&after.calls); got != 0 {
		t.Fatalf("later score plugin must never be invoked after failure, got %d calls", got)
	}
}

func TestTraceBinpackOverflowAndUnknownCapacity(t *testing.T) {
	registry := runtime.Registry{
		names.PreferenceBinPack: preferencebinpack.NewPreferenceBinPack,
	}
	fwk := newTraceTestFramework(t, registry, &fedcore.EnabledPlugins{
		ReplicasPlugins: []string{names.PreferenceBinPack},
	})

	su := framework.SchedulingUnit{
		SchedulingMode:  fedcorev1a1.SchedulingModeDivide,
		DesiredReplicas: pointer.Int64(10),
		AutoMigration: &framework.AutoMigrationSpec{
			Info: &framework.AutoMigrationInfo{
				EstimatedCapacity: map[string]int64{"cluster1": 5},
			},
		},
	}
	result, decisionTrace := runWithObserver(t, fwk, su, traceTestClusters())

	replicas := decisionTrace.Replicas
	if replicas == nil || replicas.State != schedtrace.Evaluated {
		t.Fatalf("replicas stage must be recorded as Evaluated")
	}
	if replicas.Plugin != names.PreferenceBinPack || replicas.TotalDesired != 10 {
		t.Fatalf("replicas plugin/desired total mis-recorded: %#v", replicas)
	}
	evidence := map[string]schedtrace.ReplicaClusterEvidence{}
	for _, item := range replicas.Allocation {
		evidence[item.Cluster] = item
	}
	c1, ok := evidence["cluster1"]
	if !ok {
		t.Fatalf("cluster1 evidence missing")
	}
	if c1.EstimatedCapacity == nil || *c1.EstimatedCapacity != 5 || !c1.CapacityKnown {
		t.Fatalf("known estimated capacity must be traceable, got %#v", c1)
	}
	if c1.Overflow != 5 {
		t.Fatalf("cluster1 overflow of 5 must be recorded, got %d", c1.Overflow)
	}
	if c1.Replicas != 10 {
		t.Fatalf("cluster1 keeps plan(5)+overflow(5)=10, got %d", c1.Replicas)
	}
	c2, ok := evidence["cluster2"]
	if !ok {
		t.Fatalf("cluster2 evidence missing")
	}
	if c2.EstimatedCapacity != nil || c2.CapacityKnown {
		t.Fatalf("missing capacity must stay Unknown (nil), never fabricated as zero: %#v", c2)
	}
	if c2.Replicas != 5 {
		t.Fatalf("cluster2 should receive the remaining 5 replicas, got %d", c2.Replicas)
	}
	if got := result.SuggestedClusters["cluster1"]; got == nil || *got != 10 {
		t.Fatalf("schedule result must match the traced allocation, got %v", got)
	}
}

func TestTraceDisabledProducesIdenticalScheduleAndNoExtraCalls(t *testing.T) {
	filter := &countingFilterPlugin{name: "CountingFilter"}
	score := &countingScorePlugin{
		name:          "CountingScore",
		scores:        map[string]int64{"cluster1": 4, "cluster2": 6},
		withNormalize: true,
	}
	registry := runtime.Registry{
		"CountingFilter": func(_ framework.Handle) (framework.Plugin, error) { return filter, nil },
		"CountingScore":  func(_ framework.Handle) (framework.Plugin, error) { return score, nil },
		"NaiveReplicas":  newNaiveReplicas,
	}
	enabled := &fedcore.EnabledPlugins{
		FilterPlugins:   []string{"CountingFilter"},
		ScorePlugins:    []string{"CountingScore"},
		ReplicasPlugins: []string{"NaiveReplicas"},
	}
	buildFramework := func() framework.Framework {
		return newTraceTestFramework(t, registry, enabled)
	}
	su := framework.SchedulingUnit{
		SchedulingMode:  fedcorev1a1.SchedulingModeDivide,
		DesiredReplicas: pointer.Int64(3),
	}
	clusters := traceTestClusters()

	// Baseline run with tracing disabled.
	baseline, err := NewSchedulerAlgorithm().Schedule(context.Background(), buildFramework(), su, clusters)
	if err != nil {
		t.Fatalf("baseline scheduling failed: %v", err)
	}
	baselineFilterCalls := atomic.LoadInt64(&filter.calls)
	baselineScoreCalls := atomic.LoadInt64(&score.calls)
	baselineNormalizeCalls := atomic.LoadInt64(&score.normalizeCalls)
	if baselineFilterCalls == 0 || baselineScoreCalls == 0 {
		t.Fatalf("plugin call counters must observe real invocations")
	}

	// Trace-enabled run with the same inputs.
	atomic.StoreInt64(&filter.calls, 0)
	atomic.StoreInt64(&score.calls, 0)
	atomic.StoreInt64(&score.normalizeCalls, 0)
	traced, decisionTrace := runWithObserver(t, buildFramework(), su, clusters)

	// Item-by-item identical scheduling result.
	if !reflect.DeepEqual(baseline.SuggestedClusters, traced.SuggestedClusters) {
		t.Fatalf("trace must not change scheduling results: baseline=%v traced=%v",
			baseline.SuggestedClusters, traced.SuggestedClusters)
	}
	// Identical plugin invocation counts prove tracing adds no extra calls.
	if got := atomic.LoadInt64(&filter.calls); got != baselineFilterCalls {
		t.Fatalf("filter call count differs with trace enabled: baseline=%d traced=%d",
			baselineFilterCalls, got)
	}
	if got := atomic.LoadInt64(&score.calls); got != baselineScoreCalls {
		t.Fatalf("score call count differs with trace enabled: baseline=%d traced=%d",
			baselineScoreCalls, got)
	}
	if got := atomic.LoadInt64(&score.normalizeCalls); got != baselineNormalizeCalls {
		t.Fatalf("normalize call count differs with trace enabled: baseline=%d traced=%d",
			baselineNormalizeCalls, got)
	}
	if decisionTrace.Meta.FederatedObjectUID != types.UID("fed-object-uid") ||
		decisionTrace.Meta.FederatedObjectGeneration != 3 {
		t.Fatalf("trace must be bound to the federated object UID and generation")
	}
	if len(decisionTrace.Meta.Clusters) != len(clusters) ||
		decisionTrace.Meta.Clusters[0].UID != types.UID("uid-cluster1") {
		t.Fatalf("trace must bind cluster UID snapshots")
	}
	if decisionTrace.Meta.PolicyDigest == "" {
		t.Fatalf("trace must bind a policy digest")
	}
}

func asFitError(err error, target **framework.FitError) bool {
	for err != nil {
		if fit, ok := err.(*framework.FitError); ok {
			*target = fit
			return true
		}
		type unwrapper interface{ Unwrap() error }
		unwrap, ok := err.(unwrapper)
		if !ok {
			return false
		}
		err = unwrap.Unwrap()
	}
	return false
}
