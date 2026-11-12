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

// Package trace provides SchedulingDecisionTrace, a bounded, structured
// explanation of a real scheduling run. It is built exclusively from
// framework hook observations carried through context.Context: plugins are
// never re-invoked and plugin-owned mutable data is always copied at record
// time. A nil observer is a complete no-op, so scheduling behavior is
// byte-identical when tracing is disabled.
package trace

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"regexp"
	"sort"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"

	fedcorev1a1 "github.com/kubewharf/kubeadmiral/pkg/apis/core/v1alpha1"
	"github.com/kubewharf/kubeadmiral/pkg/controllers/scheduler/framework"
)

// Stage is a scheduling framework extension point.
type Stage string

const (
	StageFilter   Stage = "Filter"
	StageScore    Stage = "Score"
	StageSelect   Stage = "Select"
	StageReplicas Stage = "Replicas"
)

// EvalState describes whether a plugin actually ran.
type EvalState string

const (
	// Evaluated means the plugin was invoked during the real scheduling run.
	Evaluated EvalState = "Evaluated"
	// NotEvaluated means the plugin was configured but not invoked, for
	// example because an earlier plugin short-circuited the extension point,
	// the plugin does not implement an optional extension such as
	// NormalizeScore, or the whole stage was skipped.
	NotEvaluated EvalState = "NotEvaluated"
)

// Outcome is the terminal result of the whole scheduling run.
type Outcome string

const (
	OutcomeSucceeded  Outcome = "Succeeded"
	OutcomeInfeasible Outcome = "Infeasible"
	OutcomeFailed     Outcome = "Failed"
	OutcomeSkipped    Outcome = "Skipped"
)

// ResultCode mirrors framework.Code as stable strings.
type ResultCode string

const (
	CodeSuccess       ResultCode = "Success"
	CodeUnschedulable ResultCode = "Unschedulable"
	CodeError         ResultCode = "Error"
)

// Budget bounds the size of a SchedulingDecisionTrace. When a budget is
// exhausted only explanatory content is truncated and the trace is explicitly
// marked; structural decision data is never silently dropped inside the
// candidate budget.
type Budget struct {
	// MaxCandidates bounds the number of candidate clusters represented in
	// the trace.
	MaxCandidates int
	// MaxLabelEntries bounds the number of labels kept per cluster snapshot.
	MaxLabelEntries int
	// MaxReasonsPerEval bounds the number of reason fragments per plugin eval.
	MaxReasonsPerEval int
	// MaxReasonBytes bounds the byte length of a single reason fragment.
	MaxReasonBytes int
	// MaxOutputBytes bounds the serialized size of Snapshot output.
	MaxOutputBytes int
}

// DefaultBudget returns the conservative default trace budget.
func DefaultBudget() Budget {
	return Budget{
		MaxCandidates:     200,
		MaxLabelEntries:   50,
		MaxReasonsPerEval: 3,
		MaxReasonBytes:    256,
		MaxOutputBytes:    64 * 1024,
	}
}

// ClusterSnapshot is an immutable copy of the cluster identity fields that
// scheduling decisions were made against.
type ClusterSnapshot struct {
	Name   string            `json:"name"`
	UID    types.UID         `json:"uid"`
	Labels map[string]string `json:"labels,omitempty"`
}

// TraceMeta binds a trace to the exact object, policy and cluster inputs of
// one scheduling run.
type TraceMeta struct {
	FederatedObjectUID        types.UID         `json:"federatedObjectUID"`
	FederatedObjectGeneration int64             `json:"federatedObjectGeneration"`
	FederatedObjectKey        string            `json:"federatedObjectKey"`
	PolicyDigest              string            `json:"policyDigest,omitempty"`
	SchedulingProfile         string            `json:"schedulingProfile,omitempty"`
	Clusters                  []ClusterSnapshot `json:"clusters"`
}

// PluginEval is the observed outcome of one plugin at one extension point.
type PluginEval struct {
	Plugin string     `json:"plugin"`
	State  EvalState  `json:"state"`
	Code   ResultCode `json:"code"`
	// Opaque is true when the decision originates from an external component
	// that did not return an internal reason. The trace records only what was
	// observable and never invents a causal explanation.
	Opaque  bool     `json:"opaque,omitempty"`
	Reasons []string `json:"reasons,omitempty"`
}

// ClusterFilterTrace records the ordered filter plugin chain for one cluster.
type ClusterFilterTrace struct {
	Cluster    string       `json:"cluster"`
	Plugins    []PluginEval `json:"plugins"`
	RejectedBy string       `json:"rejectedBy,omitempty"`
}

// FilterStageTrace is the Filter stage record.
type FilterStageTrace struct {
	Plugins          []string             `json:"plugins"`
	Clusters         []ClusterFilterTrace `json:"clusters"`
	FeasibleClusters []string             `json:"feasibleClusters"`
	RejectedClusters []string             `json:"rejectedClusters"`
}

// ClusterScoreEval records the raw score returned by a Score plugin and the
// normalized score after its NormalizeScore extension ran.
type ClusterScoreEval struct {
	Cluster         string     `json:"cluster"`
	State           EvalState  `json:"state"`
	Code            ResultCode `json:"code"`
	RawScore        int64      `json:"rawScore"`
	NormalizedScore *int64     `json:"normalizedScore,omitempty"`
	Opaque          bool       `json:"opaque,omitempty"`
	Reasons         []string   `json:"reasons,omitempty"`
}

// PluginScoreTrace is the per-plugin Score record.
type PluginScoreTrace struct {
	Plugin string `json:"plugin"`
	// Weight is the effective multiplier the framework applies to this
	// plugin's scores when aggregating the total score. The current framework
	// sums plugin scores directly, so the effective weight is always 1; it is
	// recorded explicitly so the weighting assumption is traceable.
	Weight         int64              `json:"weight"`
	NormalizeState EvalState          `json:"normalizeState"`
	Clusters       []ClusterScoreEval `json:"clusters"`
}

// AggregatedScore is the final per-cluster score after all Score plugins and
// normalization.
type AggregatedScore struct {
	Cluster      string           `json:"cluster"`
	TotalScore   int64            `json:"totalScore"`
	PluginScores map[string]int64 `json:"pluginScores"`
	// TieGroup is 0 for a cluster whose total score is unique, otherwise it is
	// the 1-based id of the equal-score group it belongs to.
	TieGroup int `json:"tieGroup"`
}

// ScoreStageTrace is the Score stage record.
type ScoreStageTrace struct {
	Plugins    []PluginScoreTrace `json:"plugins"`
	Aggregated []AggregatedScore  `json:"aggregated"`
	TieGroups  [][]string         `json:"tieGroups,omitempty"`
}

// SelectPluginTrace records one Select plugin invocation, including the
// observed input and output ordering so equal-score tie breaking is
// reconstructable without assuming a tie rule the plugin does not document.
type SelectPluginTrace struct {
	Plugin      string     `json:"plugin"`
	State       EvalState  `json:"state"`
	Code        ResultCode `json:"code"`
	Opaque      bool       `json:"opaque,omitempty"`
	Reasons     []string   `json:"reasons,omitempty"`
	InputOrder  []string   `json:"inputOrder"`
	OutputOrder []string   `json:"outputOrder"`
	Eliminated  []string   `json:"eliminated,omitempty"`
}

// SelectStageTrace is the Select stage record.
type SelectStageTrace struct {
	Plugins    []SelectPluginTrace `json:"plugins"`
	FinalOrder []string            `json:"finalOrder"`
	TieGroups  [][]string          `json:"tieGroups,omitempty"`
}

// ReplicaClusterEvidence explains the replica allocation of one cluster.
// Nil capacity pointers mean the capacity was unknown at scheduling time
// rather than zero.
type ReplicaClusterEvidence struct {
	Cluster           string `json:"cluster"`
	Replicas          int64  `json:"replicas"`
	Overflow          int64  `json:"overflow,omitempty"`
	EstimatedCapacity *int64 `json:"estimatedCapacity,omitempty"`
	LimitedCapacity   *int64 `json:"limitedCapacity,omitempty"`
	MinReplicas       int64  `json:"minReplicas"`
	MaxReplicas       *int64 `json:"maxReplicas,omitempty"`
	CapacityKnown     bool   `json:"capacityKnown"`
}

// ReplicasStageTrace is the Replicas stage record.
type ReplicasStageTrace struct {
	State          EvalState                `json:"state"`
	SkipReason     string                   `json:"skipReason,omitempty"`
	Plugin         string                   `json:"plugin,omitempty"`
	Code           ResultCode               `json:"code"`
	TotalDesired   int64                    `json:"totalDesired"`
	TotalAllocated int64                    `json:"totalAllocated"`
	Unscheduled    int64                    `json:"unscheduled,omitempty"`
	Allocation     []ReplicaClusterEvidence `json:"allocation"`
}

// SchedulingDecisionTrace is the finite, structured explanation of one real
// scheduling run.
type SchedulingDecisionTrace struct {
	Meta              TraceMeta           `json:"meta"`
	StartedAt         time.Time           `json:"startedAt"`
	FinishedAt        time.Time           `json:"finishedAt,omitempty"`
	Outcome           Outcome             `json:"outcome"`
	TerminalStage     Stage               `json:"terminalStage,omitempty"`
	SkipReason        string              `json:"skipReason,omitempty"`
	Truncated         bool                `json:"truncated"`
	TruncationReasons []string            `json:"truncationReasons,omitempty"`
	Filter            *FilterStageTrace   `json:"filter,omitempty"`
	Score             *ScoreStageTrace    `json:"score,omitempty"`
	Select            *SelectStageTrace   `json:"select,omitempty"`
	Replicas          *ReplicasStageTrace `json:"replicas,omitempty"`
}

// OpaqueDecisionPlugin is implemented by plugins whose decisions rely on an
// external system (such as a scheduling webhook) that may not return an
// internal reason. Observations of these plugins are marked Opaque and only
// externally observable facts are recorded.
type OpaqueDecisionPlugin interface {
	SchedulingDecisionOpaque()
}

// IsOpaque reports whether the plugin's rationale must be treated as opaque.
func IsOpaque(plugin framework.Plugin) bool {
	if plugin == nil {
		return false
	}
	_, ok := plugin.(OpaqueDecisionPlugin)
	return ok
}

// DigestStruct returns the sha256 hex digest of the JSON encoding of v. It is
// used to bind a trace to a stable digest of its scheduling policy.
func DigestStruct(v any) string {
	encoded, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

// BuildClusterSnapshots copies identity fields (UID and a bounded label
// snapshot) from candidate clusters. Cluster pointers are never retained.
func BuildClusterSnapshots(clusters []*fedcorev1a1.FederatedCluster, budget Budget) []ClusterSnapshot {
	if len(clusters) == 0 {
		return nil
	}
	limit := budget.MaxCandidates
	if limit <= 0 || limit > len(clusters) {
		limit = len(clusters)
	}
	snapshots := make([]ClusterSnapshot, 0, limit)
	for i := 0; i < limit; i++ {
		cluster := clusters[i]
		snapshots = append(snapshots, ClusterSnapshot{
			Name:   cluster.Name,
			UID:    cluster.UID,
			Labels: copyLabels(cluster.Labels, budget.MaxLabelEntries),
		})
	}
	return snapshots
}

var (
	// sensitiveContentRegex matches reason fragments that refer to material
	// that must never be exported verbatim.
	sensitiveContentRegex = regexp.MustCompile(`(?i)(secret|password|passwd|token|credential|api[_-]?key)`)
	// sensitiveEnvRegex matches NAME=value style fragments whose name looks
	// sensitive, e.g. DB_PASSWORD=abc.
	sensitiveEnvRegex = regexp.MustCompile(`(?i)^[A-Z0-9_]*(SECRET|TOKEN|PASSWORD|PASSWD|CREDENTIAL|API[_-]?KEY)[A-Z0-9_]*=`)
)

const redactionMarker = "<redacted:sensitive-content>"

// sanitizeReasons copies, redacts and bounds plugin reason fragments. It
// guarantees that secrets, full pod environment content and unbounded label
// material can never leak into a trace through reason strings.
func sanitizeReasons(reasons []string, budget Budget) []string {
	if len(reasons) == 0 {
		return nil
	}
	limit := budget.MaxReasonsPerEval
	if limit <= 0 || limit > len(reasons) {
		limit = len(reasons)
	}
	out := make([]string, 0, limit)
	for i := 0; i < limit; i++ {
		fragment := reasons[i]
		switch {
		case sensitiveContentRegex.MatchString(fragment):
			out = append(out, redactionMarker)
		case sensitiveEnvRegex.MatchString(fragment):
			out = append(out, redactionMarker)
		default:
			out = append(out, truncate(fragment, budget.MaxReasonBytes))
		}
	}
	return out
}

func truncate(value string, maxBytes int) string {
	if maxBytes <= 0 {
		return value
	}
	if len(value) <= maxBytes {
		return value
	}
	return value[:maxBytes] + "...<truncated>"
}

func copyLabels(labels map[string]string, maxEntries int) map[string]string {
	if len(labels) == 0 {
		return nil
	}
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if maxEntries > 0 && len(keys) > maxEntries {
		keys = keys[:maxEntries]
	}
	copied := make(map[string]string, len(keys))
	for _, key := range keys {
		if sensitiveContentRegex.MatchString(key) {
			copied[key] = redactionMarker
			continue
		}
		copied[key] = labels[key]
	}
	return copied
}

func int64Ptr(value int64) *int64 {
	return &value
}

// ResultCodeOf converts a framework.Result to its stable trace code. A nil
// result means Success.
func ResultCodeOf(result *framework.Result) ResultCode {
	if result == nil || result.IsSuccess() {
		return CodeSuccess
	}
	switch result.Code() {
	case framework.Unschedulable:
		return CodeUnschedulable
	case framework.Error:
		return CodeError
	default:
		return CodeError
	}
}

type observerContextKey struct{}

// WithObserver returns a context carrying the given observer.
func WithObserver(ctx context.Context, observer *Observer) context.Context {
	if observer == nil {
		return ctx
	}
	return context.WithValue(ctx, observerContextKey{}, observer)
}

// ObserverFromContext returns the observer bound to ctx, or nil when tracing
// is disabled. All recorder entry points treat nil as a no-op.
func ObserverFromContext(ctx context.Context) *Observer {
	if ctx == nil {
		return nil
	}
	observer, _ := ctx.Value(observerContextKey{}).(*Observer)
	return observer
}

// Observer accumulates a SchedulingDecisionTrace from real framework hook
// observations. It is safe for concurrent use: its lock is only held while
// copying value-type inputs, it never calls back into the framework or
// plugins, and recorder failures can never alter scheduling control flow.
type Observer struct {
	mu      sync.Mutex
	budget  Budget
	trace   *SchedulingDecisionTrace
	index   map[string]int // cluster name -> filter stage entry index
	replica map[string]int // cluster name -> replica evidence index
}

// NewObserver creates an observer initialized with immutable run metadata.
// A nil budget means DefaultBudget.
func NewObserver(meta TraceMeta, budget *Budget) *Observer {
	b := DefaultBudget()
	if budget != nil {
		b = *budget
	}
	return &Observer{
		budget:  b,
		trace:   &SchedulingDecisionTrace{Meta: cloneMeta(meta)},
		index:   make(map[string]int),
		replica: make(map[string]int),
	}
}

func (o *Observer) mutate(fn func(t *SchedulingDecisionTrace)) {
	if o == nil {
		return
	}
	// Defensive guarantee: observer code must never disrupt a scheduling run.
	defer func() {
		_ = recover()
	}()
	o.mu.Lock()
	defer o.mu.Unlock()
	fn(o.trace)
}

// Begin marks the start of the scheduling run.
func (o *Observer) Begin() {
	o.mutate(func(t *SchedulingDecisionTrace) {
		if t.StartedAt.IsZero() {
			t.StartedAt = time.Now()
		}
	})
}

// RecordScheduleSkipped records a run that returned before any framework
// stage, for example a sticky cluster reschedule bypass.
func (o *Observer) RecordScheduleSkipped(reason string) {
	o.mutate(func(t *SchedulingDecisionTrace) {
		t.Outcome = OutcomeSkipped
		t.SkipReason = truncate(reason, o.budget.MaxReasonBytes)
		if t.StartedAt.IsZero() {
			t.StartedAt = time.Now()
		}
		t.FinishedAt = time.Now()
	})
}

// Finish finalizes the run outcome and, for failed or infeasible runs, the
// stage that terminated scheduling.
func (o *Observer) Finish(outcome Outcome, terminalStage Stage) {
	o.mutate(func(t *SchedulingDecisionTrace) {
		t.Outcome = outcome
		t.TerminalStage = terminalStage
		if t.StartedAt.IsZero() {
			t.StartedAt = time.Now()
		}
		t.FinishedAt = time.Now()
	})
}

// MarkTruncated records that a budget was exhausted and what was truncated.
func (o *Observer) MarkTruncated(reason string) {
	o.mutate(func(t *SchedulingDecisionTrace) {
		t.Truncated = true
		t.TruncationReasons = append(t.TruncationReasons, truncate(reason, o.budget.MaxReasonBytes))
	})
}

func (o *Observer) filterStage(t *SchedulingDecisionTrace) *FilterStageTrace {
	if t.Filter == nil {
		t.Filter = &FilterStageTrace{}
	}
	return t.Filter
}

// RecordFilterPluginOrder records the configured filter plugin execution
// order. It is idempotent.
func (o *Observer) RecordFilterPluginOrder(plugins []string) {
	o.mutate(func(t *SchedulingDecisionTrace) {
		stage := o.filterStage(t)
		if len(stage.Plugins) == 0 {
			stage.Plugins = append(stage.Plugins, plugins...)
		}
	})
}

// RecordFilterEval appends one plugin evaluation for one cluster, creating
// the cluster entry on first touch. NotEvaluated evals are used for plugins
// skipped by filter short-circuiting.
func (o *Observer) RecordFilterEval(clusterName string, eval PluginEval) {
	o.mutate(func(t *SchedulingDecisionTrace) {
		stage := o.filterStage(t)
		if _, exists := o.index[clusterName]; !exists {
			if o.budget.MaxCandidates > 0 && len(stage.Clusters) >= o.budget.MaxCandidates {
				t.Truncated = true
				t.TruncationReasons = append(t.TruncationReasons, "candidateBudgetExceeded:filter")
				return
			}
			stage.Clusters = append(stage.Clusters, ClusterFilterTrace{Cluster: clusterName})
			o.index[clusterName] = len(stage.Clusters) - 1
		}
		entry := &stage.Clusters[o.index[clusterName]]
		entry.Plugins = append(entry.Plugins, o.sanitizeEval(eval))
		if entry.RejectedBy == "" && eval.State == Evaluated && eval.Code != CodeSuccess {
			entry.RejectedBy = eval.Plugin
		}
	})
}

// RecordFilterOutcome records the feasible and rejected cluster name sets.
func (o *Observer) RecordFilterOutcome(feasible, rejected []string) {
	o.mutate(func(t *SchedulingDecisionTrace) {
		stage := o.filterStage(t)
		stage.FeasibleClusters = append([]string(nil), feasible...)
		stage.RejectedClusters = append([]string(nil), rejected...)
	})
}

func (o *Observer) scoreStage(t *SchedulingDecisionTrace) *ScoreStageTrace {
	if t.Score == nil {
		t.Score = &ScoreStageTrace{}
	}
	return t.Score
}

// RecordScorePlugin records one Score plugin: its effective weight, whether
// NormalizeScore ran, and per-cluster raw/normalized scores. All scores are
// value copies taken at the hook, so later in-place mutation by shared plugin
// instances cannot affect the trace.
func (o *Observer) RecordScorePlugin(pluginTrace PluginScoreTrace) {
	o.mutate(func(t *SchedulingDecisionTrace) {
		stage := o.scoreStage(t)
		copied := PluginScoreTrace{
			Plugin:         pluginTrace.Plugin,
			Weight:         pluginTrace.Weight,
			NormalizeState: pluginTrace.NormalizeState,
			Clusters:       make([]ClusterScoreEval, 0, len(pluginTrace.Clusters)),
		}
		for _, eval := range pluginTrace.Clusters {
			copied.Clusters = append(copied.Clusters, o.sanitizeScoreEval(eval))
		}
		stage.Plugins = append(stage.Plugins, copied)
	})
}

// RecordScoreAggregation records the summed per-cluster totals and equal-score
// tie groups observed after all Score plugins ran.
func (o *Observer) RecordScoreAggregation(aggregated []AggregatedScore, tieGroups [][]string) {
	o.mutate(func(t *SchedulingDecisionTrace) {
		stage := o.scoreStage(t)
		stage.Aggregated = make([]AggregatedScore, 0, len(aggregated))
		for _, item := range aggregated {
			copied := AggregatedScore{
				Cluster:      item.Cluster,
				TotalScore:   item.TotalScore,
				TieGroup:     item.TieGroup,
				PluginScores: make(map[string]int64, len(item.PluginScores)),
			}
			for plugin, score := range item.PluginScores {
				copied.PluginScores[plugin] = score
			}
			stage.Aggregated = append(stage.Aggregated, copied)
		}
		stage.TieGroups = cloneStringGroups(tieGroups)
	})
}

func (o *Observer) selectStage(t *SchedulingDecisionTrace) *SelectStageTrace {
	if t.Select == nil {
		t.Select = &SelectStageTrace{}
	}
	return t.Select
}

// RecordSelectPlugin records one Select plugin invocation with observed
// ordering and eliminations.
func (o *Observer) RecordSelectPlugin(pluginTrace SelectPluginTrace) {
	o.mutate(func(t *SchedulingDecisionTrace) {
		stage := o.selectStage(t)
		stage.Plugins = append(stage.Plugins, SelectPluginTrace{
			Plugin:      pluginTrace.Plugin,
			State:       pluginTrace.State,
			Code:        pluginTrace.Code,
			Opaque:      pluginTrace.Opaque,
			Reasons:     sanitizeReasons(pluginTrace.Reasons, o.budget),
			InputOrder:  append([]string(nil), pluginTrace.InputOrder...),
			OutputOrder: append([]string(nil), pluginTrace.OutputOrder...),
			Eliminated:  append([]string(nil), pluginTrace.Eliminated...),
		})
		stage.FinalOrder = append([]string(nil), pluginTrace.OutputOrder...)
	})
}

// RecordSelectTieGroups records equal-score groups visible at Select input.
func (o *Observer) RecordSelectTieGroups(tieGroups [][]string) {
	o.mutate(func(t *SchedulingDecisionTrace) {
		o.selectStage(t).TieGroups = cloneStringGroups(tieGroups)
	})
}

func (o *Observer) replicasStage(t *SchedulingDecisionTrace) *ReplicasStageTrace {
	if t.Replicas == nil {
		t.Replicas = &ReplicasStageTrace{Allocation: []ReplicaClusterEvidence{}}
	}
	return t.Replicas
}

// RecordReplicasSkipped records that the Replicas stage did not run, with the
// observed skip reason.
func (o *Observer) RecordReplicasSkipped(reason string) {
	o.mutate(func(t *SchedulingDecisionTrace) {
		stage := o.replicasStage(t)
		stage.State = NotEvaluated
		stage.SkipReason = truncate(reason, o.budget.MaxReasonBytes)
	})
}

// RecordReplicasPlugin records the replicas plugin invocation and its result
// code. Allocation entries are value copies of the observed replica list.
func (o *Observer) RecordReplicasPlugin(
	plugin string,
	code ResultCode,
	desired int64,
	allocation map[string]int64,
) {
	o.mutate(func(t *SchedulingDecisionTrace) {
		stage := o.replicasStage(t)
		stage.State = Evaluated
		stage.Plugin = plugin
		stage.Code = code
		stage.TotalDesired = desired
		var total int64
		for cluster, replicas := range allocation {
			total += replicas
			o.upsertEvidence(stage, ReplicaClusterEvidence{Cluster: cluster, Replicas: replicas})
		}
		stage.TotalAllocated = total
		if desired > total {
			stage.Unscheduled = desired - total
		}
	})
}

// RecordReplicaEvidence enriches allocation entries with the capacity and
// overflow evidence observed inside a replicas plugin. The plugin is not
// re-invoked: callers pass values already computed during the real run.
func (o *Observer) RecordReplicaEvidence(evidences []ReplicaClusterEvidence) {
	o.mutate(func(t *SchedulingDecisionTrace) {
		stage := o.replicasStage(t)
		for _, evidence := range evidences {
			copied := ReplicaClusterEvidence{
				Cluster:           evidence.Cluster,
				Replicas:          evidence.Replicas,
				Overflow:          evidence.Overflow,
				EstimatedCapacity: copyInt64Ptr(evidence.EstimatedCapacity),
				LimitedCapacity:   copyInt64Ptr(evidence.LimitedCapacity),
				MinReplicas:       evidence.MinReplicas,
				MaxReplicas:       copyInt64Ptr(evidence.MaxReplicas),
				CapacityKnown:     evidence.CapacityKnown,
			}
			o.upsertEvidence(stage, copied)
		}
	})
}

func (o *Observer) upsertEvidence(stage *ReplicasStageTrace, evidence ReplicaClusterEvidence) {
	if idx, exists := o.replica[evidence.Cluster]; exists {
		mergeEvidence(&stage.Allocation[idx], evidence)
		return
	}
	if o.budget.MaxCandidates > 0 && len(stage.Allocation) >= o.budget.MaxCandidates {
		o.trace.Truncated = true
		o.trace.TruncationReasons = append(o.trace.TruncationReasons, "candidateBudgetExceeded:replicas")
		return
	}
	stage.Allocation = append(stage.Allocation, evidence)
	o.replica[evidence.Cluster] = len(stage.Allocation) - 1
}

func mergeEvidence(dst *ReplicaClusterEvidence, src ReplicaClusterEvidence) {
	if src.Replicas != 0 {
		dst.Replicas = src.Replicas
	}
	if src.Overflow != 0 {
		dst.Overflow = src.Overflow
	}
	if src.EstimatedCapacity != nil {
		dst.EstimatedCapacity = copyInt64Ptr(src.EstimatedCapacity)
		dst.CapacityKnown = true
	}
	if src.LimitedCapacity != nil {
		dst.LimitedCapacity = copyInt64Ptr(src.LimitedCapacity)
	}
	if src.MinReplicas != 0 {
		dst.MinReplicas = src.MinReplicas
	}
	if src.MaxReplicas != nil {
		dst.MaxReplicas = copyInt64Ptr(src.MaxReplicas)
	}
	if src.CapacityKnown {
		dst.CapacityKnown = true
	}
}

func (o *Observer) sanitizeEval(eval PluginEval) PluginEval {
	return PluginEval{
		Plugin:  eval.Plugin,
		State:   eval.State,
		Code:    eval.Code,
		Opaque:  eval.Opaque,
		Reasons: sanitizeReasons(eval.Reasons, o.budget),
	}
}

func (o *Observer) sanitizeScoreEval(eval ClusterScoreEval) ClusterScoreEval {
	return ClusterScoreEval{
		Cluster:         eval.Cluster,
		State:           eval.State,
		Code:            eval.Code,
		RawScore:        eval.RawScore,
		NormalizedScore: copyInt64Ptr(eval.NormalizedScore),
		Opaque:          eval.Opaque,
		Reasons:         sanitizeReasons(eval.Reasons, o.budget),
	}
}

// Snapshot returns a deep, frozen copy of the accumulated trace. When the
// serialized trace exceeds MaxOutputBytes, explanatory reason fields are
// truncated (never structural decision data) and Truncated is set with the
// reason.
func (o *Observer) Snapshot() *SchedulingDecisionTrace {
	if o == nil {
		return nil
	}
	o.mu.Lock()
	copied := cloneTrace(o.trace)
	budget := o.budget
	o.mu.Unlock()

	encoded, err := json.Marshal(copied)
	if err == nil && budget.MaxOutputBytes > 0 && len(encoded) > budget.MaxOutputBytes {
		stripExplanations(copied)
		copied.Truncated = true
		copied.TruncationReasons = append(copied.TruncationReasons, "outputByteBudgetExceeded:explanationsTruncated")
	}
	return copied
}

func stripExplanations(t *SchedulingDecisionTrace) {
	if t.Filter != nil {
		for i := range t.Filter.Clusters {
			for j := range t.Filter.Clusters[i].Plugins {
				t.Filter.Clusters[i].Plugins[j].Reasons = nil
			}
		}
	}
	if t.Score != nil {
		for i := range t.Score.Plugins {
			for j := range t.Score.Plugins[i].Clusters {
				t.Score.Plugins[i].Clusters[j].Reasons = nil
			}
		}
	}
	if t.Select != nil {
		for i := range t.Select.Plugins {
			t.Select.Plugins[i].Reasons = nil
		}
	}
	if t.Replicas != nil {
		t.Replicas.SkipReason = ""
	}
	t.SkipReason = ""
}

func cloneMeta(meta TraceMeta) TraceMeta {
	copied := TraceMeta{
		FederatedObjectUID:        meta.FederatedObjectUID,
		FederatedObjectGeneration: meta.FederatedObjectGeneration,
		FederatedObjectKey:        meta.FederatedObjectKey,
		PolicyDigest:              meta.PolicyDigest,
		SchedulingProfile:         meta.SchedulingProfile,
		Clusters:                  make([]ClusterSnapshot, 0, len(meta.Clusters)),
	}
	for _, cluster := range meta.Clusters {
		copied.Clusters = append(copied.Clusters, ClusterSnapshot{
			Name:   cluster.Name,
			UID:    cluster.UID,
			Labels: copyStringMap(cluster.Labels),
		})
	}
	return copied
}

func cloneTrace(in *SchedulingDecisionTrace) *SchedulingDecisionTrace {
	if in == nil {
		return nil
	}
	encoded, err := json.Marshal(in)
	if err != nil {
		return &SchedulingDecisionTrace{
			Meta: cloneMeta(in.Meta), Truncated: true,
			TruncationReasons: []string{"snapshotMarshalFailed"},
		}
	}
	out := &SchedulingDecisionTrace{}
	if err := json.Unmarshal(encoded, out); err != nil {
		return &SchedulingDecisionTrace{
			Meta: cloneMeta(in.Meta), Truncated: true,
			TruncationReasons: []string{"snapshotUnmarshalFailed"},
		}
	}
	return out
}

func cloneStringGroups(groups [][]string) [][]string {
	if len(groups) == 0 {
		return nil
	}
	copied := make([][]string, 0, len(groups))
	for _, group := range groups {
		copied = append(copied, append([]string(nil), group...))
	}
	return copied
}

func copyStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func copyInt64Ptr(in *int64) *int64 {
	if in == nil {
		return nil
	}
	return int64Ptr(*in)
}
