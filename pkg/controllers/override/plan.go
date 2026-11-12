/*
Copyright 2026 The KubeAdmiral Authors.

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

package override

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"

	fedcorev1a1 "github.com/kubewharf/kubeadmiral/pkg/apis/core/v1alpha1"
	ctrutil "github.com/kubewharf/kubeadmiral/pkg/controllers/util"
	podutil "github.com/kubewharf/kubeadmiral/pkg/util/pod"
)

// PlanMode controls how the planner reacts to conflicts it can detect ahead of application.
type PlanMode string

const (
	// PlanModeCompatible (the default) preserves the historical last-writer semantics: conflicts are
	// annotated on the plan but never rejected.
	PlanModeCompatible PlanMode = "Compatible"
	// PlanModeStrict rejects definite write/write conflicts, undecidable conflicts and patch application
	// failures instead of silently keeping last-writer behavior.
	PlanModeStrict PlanMode = "Strict"
)

const (
	defaultMaxPatches          = 500
	defaultMaxPathBytes        = 8 * 1024
	defaultMaxPatchValueBytes  = 256 * 1024
	defaultMaxCandidateBytes   = 4 * 1024 * 1024
	finalizersPathPrefix       = "/metadata/finalizers"
	overriderTypeImage         = "image"
	overriderTypeCommand       = "command"
	overriderTypeArgs          = "args"
	overriderTypeEnvs          = "envs"
	overriderTypeAnnotations   = "annotations"
	overriderTypeLabels        = "labels"
	overriderTypeJSONPatch     = "jsonpatch"
	redactionMarker            = "***REDACTED***"
	unknownEffectivePathMarker = "<unknown>"
	removedEffectivePathMarker = "<removed>"
)

// Sentinel errors returned by the planner. Callers should use errors.Is to inspect them.
var (
	// ErrPlanBindingMismatch indicates that the inputs presented to Apply no longer match the inputs the
	// plan was bound to (UID, resourceVersion or digest).
	ErrPlanBindingMismatch = errors.New("override patch plan binding mismatch")
	// ErrPlanStale indicates that re-planning the current inputs no longer produces the same patches or
	// candidate object.
	ErrPlanStale = errors.New("override patch plan is stale")
	// ErrPlanLimitExceeded indicates that a plan exceeded one of the configured byte/count guards.
	ErrPlanLimitExceeded = errors.New("override patch plan limit exceeded")
)

// PlanLimits bounds the resources a single plan may consume. The zero value is replaced by the defaults.
type PlanLimits struct {
	// MaxPatches bounds the number of raw patches produced for a single cluster.
	MaxPatches int `json:"maxPatches"`
	// MaxPathBytes bounds the byte length of a single JSON pointer.
	MaxPathBytes int `json:"maxPathBytes"`
	// MaxPatchValueBytes bounds the byte length of a single patch value.
	MaxPatchValueBytes int `json:"maxPatchValueBytes"`
	// MaxCandidateBytes bounds the byte length of the candidate object.
	MaxCandidateBytes int64 `json:"maxCandidateBytes"`
}

func defaultPlanLimits() PlanLimits {
	return PlanLimits{
		MaxPatches:         defaultMaxPatches,
		MaxPathBytes:       defaultMaxPathBytes,
		MaxPatchValueBytes: defaultMaxPatchValueBytes,
		MaxCandidateBytes:  defaultMaxCandidateBytes,
	}
}

// planConfig is the resolved, private configuration carried by a plan so that Apply can replay it exactly.
type planConfig struct {
	mode   PlanMode
	limits PlanLimits
}

// PlanOption customizes plan generation.
type PlanOption func(*planConfig)

// WithStrictMode enables the strict rejection mode.
func WithStrictMode() PlanOption {
	return func(c *planConfig) {
		c.mode = PlanModeStrict
	}
}

// WithPlanLimits overrides the default resource guards. Non-positive fields keep their default values.
func WithPlanLimits(limits PlanLimits) PlanOption {
	return func(c *planConfig) {
		if limits.MaxPatches > 0 {
			c.limits.MaxPatches = limits.MaxPatches
		}
		if limits.MaxPathBytes > 0 {
			c.limits.MaxPathBytes = limits.MaxPathBytes
		}
		if limits.MaxPatchValueBytes > 0 {
			c.limits.MaxPatchValueBytes = limits.MaxPatchValueBytes
		}
		if limits.MaxCandidateBytes > 0 {
			c.limits.MaxCandidateBytes = limits.MaxCandidateBytes
		}
	}
}

// PatchSource identifies the policy rule and typed overrider that produced a raw patch. It is the
// replayable provenance of every entry in the ordered patch list.
type PatchSource struct {
	// PolicyKind is either OverridePolicy or ClusterOverridePolicy.
	PolicyKind string `json:"policyKind"`
	// PolicyKey is the namespaced key (namespace/name) or cluster-scoped name of the policy.
	PolicyKey string `json:"policyKey"`
	// RuleIndex is the zero-based index of the OverrideRule inside the policy spec.
	RuleIndex int `json:"ruleIndex"`
	// OverriderType is one of image, command, args, envs, annotations, labels or jsonpatch.
	OverriderType string `json:"overriderType"`
	// OverriderIndexes lists the typed overrider items that contributed to the patch. Coalesced typed
	// patches may carry several indexes; jsonpatch patches always carry a single index.
	OverriderIndexes []int `json:"overriderIndexes,omitempty"`
	// ContainerName is the real container resolved from the live pod spec for container-targeting
	// overriders. It is empty for non-container patches.
	ContainerName string `json:"containerName,omitempty"`
}

// PlannedPatch is a single raw JSON patch together with its provenance and its static analysis result.
type PlannedPatch struct {
	// Index is the global zero-based position of the patch in the ordered raw patch list of the cluster.
	Index int `json:"index"`
	// Patch is the raw RFC 6902-ish patch. It is byte-compatible with the parseOverrides pipeline.
	Patch fedcorev1a1.OverridePatch `json:"patch"`
	// Source records where the patch came from.
	Source PatchSource `json:"source"`
	// Deferred is true for finalizer patches, which the existing pipeline deliberately excludes from
	// template application (they are applied later by the sync controller).
	Deferred bool `json:"deferred"`
	// EffectivePath is the pointer the declared path denotes in the document after all later array
	// mutations have been applied. It equals Patch.Path when nothing relocates it.
	EffectivePath string `json:"effectivePath,omitempty"`
	// ReadPaths are the JSON pointers read by the operation at execution time.
	ReadPaths []string `json:"readPaths,omitempty"`
	// WritePaths are the JSON pointers written by the operation at execution time.
	WritePaths []string `json:"writePaths,omitempty"`
	// Conflicts are conflicts detected against earlier patches, interpreted in real execution order.
	Conflicts []PatchConflict `json:"conflicts,omitempty"`
}

// ConflictKind classifies a detected conflict.
type ConflictKind string

const (
	// ConflictWriteWrite describes two writes that touch the same node or overlapping parent/child
	// pointers. The later patch wins under the historical last-writer semantics.
	ConflictWriteWrite ConflictKind = "WriteWrite"
	// ConflictDependency describes an accurately resolved ordering dependency between a read and a
	// write (including test/move/copy style accesses). It is informational and replayable.
	ConflictDependency ConflictKind = "Dependency"
	// ConflictUnknown marks a relationship that cannot be decided precisely (for example an index that
	// was removed by an earlier array mutation or an unsupported move/copy without a from pointer).
	ConflictUnknown ConflictKind = "UnknownConflict"
)

// PatchConflict describes the relationship between a patch and an earlier patch.
type PatchConflict struct {
	// Kind classifies the conflict.
	Kind ConflictKind `json:"kind"`
	// EarlierPatchIndex is the index of the earlier patch inside the cluster patch list.
	EarlierPatchIndex int `json:"earlierPatchIndex"`
	// EarlierSource is the provenance of the earlier patch.
	EarlierSource PatchSource `json:"earlierSource"`
	// Path is the pointer (in current-document coordinates where known) involved in the conflict.
	Path string `json:"path"`
	// Reason is a human-readable, deterministic explanation of the relationship.
	Reason string `json:"reason"`
}

// ClusterPatchPlan is the per-cluster planning result.
type ClusterPatchPlan struct {
	// Cluster is the member cluster name.
	Cluster string `json:"cluster"`
	// Patches is the ordered raw patch list, including deferred finalizer patches.
	Patches []PlannedPatch `json:"patches"`
	// ReadPaths is the sorted union of all read pointers.
	ReadPaths []string `json:"readPaths,omitempty"`
	// WritePaths is the sorted union of all write pointers.
	WritePaths []string `json:"writePaths,omitempty"`
	// Conflicts is the deterministic aggregation of all patch conflicts, ordered by patch index.
	Conflicts []PatchConflict `json:"conflicts,omitempty"`
	// Candidate is the fully resolved candidate object with secret values redacted. It is nil when no
	// patch applies. The unredacted bytes remain available through CandidateBytes for size accounting.
	Candidate *unstructured.Unstructured `json:"candidate,omitempty"`
	// CandidateBytes is the raw (unredacted) byte length of the candidate object.
	CandidateBytes int64 `json:"candidateBytes"`
	// Redacted reports whether the exported Candidate had any secret value redacted.
	Redacted bool `json:"redacted"`
	// FailedAtIndex is the index of the first patch that failed to apply, or -1 when all applicable
	// patches applied successfully.
	FailedAtIndex int `json:"failedAtIndex"`
	// FailureReason records the application error, if any.
	FailureReason string `json:"failureReason,omitempty"`

	// candidateRaw retains the unredacted candidate bytes for in-package verification and size
	// accounting. The exported Candidate is always redacted.
	candidateRaw []byte
}

// PolicyBinding binds a matched policy snapshot to the plan.
type PolicyBinding struct {
	// Kind is OverridePolicy or ClusterOverridePolicy.
	Kind string `json:"kind"`
	// Key is the policy key.
	Key string `json:"key"`
	// UID is the policy UID at plan time.
	UID types.UID `json:"uid"`
	// ResourceVersion is the policy resource version at plan time.
	ResourceVersion string `json:"resourceVersion"`
	// Digest is the sha256 digest of kind, key, identity and spec.
	Digest string `json:"digest"`
}

// ClusterBinding binds a cluster snapshot to the plan. Only name and labels (the fields that drive
// cluster matching) participate in the digest.
type ClusterBinding struct {
	// Name is the cluster name.
	Name string `json:"name"`
	// Digest is the sha256 digest of name and labels.
	Digest string `json:"digest"`
}

// OverridePatchPlan is a zero-write preview of the ordered raw patches, typed provenance, read/write
// pointer sets and candidate objects produced by the matched override policies for a fixed snapshot of
// a FederatedObject and its member clusters. Building a plan never mutates the FederatedObject, its
// template, its overrides, the policies or the clusters.
type OverridePatchPlan struct {
	// Mode is the conflict handling mode in effect.
	Mode PlanMode `json:"mode"`
	// Limits are the resource guards enforced while building the plan.
	Limits PlanLimits `json:"limits"`
	// FedObjectKey is the qualified key of the planned FederatedObject.
	FedObjectKey string `json:"fedObjectKey"`
	// FedObjectUID binds the plan to a concrete FederatedObject.
	FedObjectUID types.UID `json:"fedObjectUid"`
	// FedObjectResourceVersion binds the plan to the observed FederatedObject version.
	FedObjectResourceVersion string `json:"fedObjectResourceVersion"`
	// TemplateDigest is the sha256 digest of the raw template.
	TemplateDigest string `json:"templateDigest"`
	// Policies binds the matched policy snapshots in application order.
	Policies []PolicyBinding `json:"policies"`
	// Clusters binds the cluster snapshots covered by the plan.
	Clusters []ClusterBinding `json:"clusters"`
	// ClusterPlans holds the per-cluster results.
	ClusterPlans []ClusterPatchPlan `json:"clusterPlans"`
	// CreatedAt is when the plan was built.
	CreatedAt metav1.Time `json:"createdAt"`

	// config is retained to allow Apply to replay the exact same options.
	config planConfig
}

// StrictConflictError is returned in strict mode when the plan contains rejectable conflicts or
// application failures. The plan is still attached for inspection.
type StrictConflictError struct {
	// Plan is the fully populated plan that triggered the rejection.
	Plan *OverridePatchPlan
	// Reasons is the deterministic list of rejection reasons.
	Reasons []string
}

// Error implements error.
func (e *StrictConflictError) Error() string {
	return fmt.Sprintf("strict override patch plan rejected %d conflict(s): %s", len(e.Reasons), strings.Join(e.Reasons, "; "))
}

// PlanOverrides builds an OverridePatchPlan for a fixed FederatedObject snapshot, an ordered list of
// matched policies and a snapshot of member clusters. It performs no API writes and never mutates its
// inputs. The policy order, cluster selector/affinity matching and finalizer filtering rules are
// identical to the parseOverrides pipeline.
func PlanOverrides(
	fedObject fedcorev1a1.GenericFederatedObject,
	policies []fedcorev1a1.GenericOverridePolicy,
	clusters []*fedcorev1a1.FederatedCluster,
	opts ...PlanOption,
) (*OverridePatchPlan, error) {
	if fedObject == nil {
		return nil, errors.New("fedObject is nil")
	}

	cfg := planConfig{mode: PlanModeCompatible, limits: defaultPlanLimits()}
	for _, opt := range opts {
		opt(&cfg)
	}

	templateRaw := fedObject.GetSpec().Template.Raw
	if len(templateRaw) == 0 {
		return nil, fmt.Errorf("fedObject %q has an empty template", fedObjectKey(fedObject))
	}

	plan := &OverridePatchPlan{
		Mode:                     cfg.mode,
		Limits:                   cfg.limits,
		FedObjectKey:             fedObjectKey(fedObject),
		FedObjectUID:             fedObject.GetUID(),
		FedObjectResourceVersion: fedObject.GetResourceVersion(),
		TemplateDigest:           sha256Hex(templateRaw),
		Policies:                 make([]PolicyBinding, 0, len(policies)),
		Clusters:                 make([]ClusterBinding, 0, len(clusters)),
		ClusterPlans:             make([]ClusterPatchPlan, 0, len(clusters)),
		CreatedAt:                metav1.Now(),
		config:                   cfg,
	}

	for _, policy := range policies {
		digest, err := digestPolicy(policy)
		if err != nil {
			return nil, err
		}
		plan.Policies = append(plan.Policies, PolicyBinding{
			Kind:            policyKind(policy),
			Key:             policy.GetKey(),
			UID:             policy.GetUID(),
			ResourceVersion: policy.GetResourceVersion(),
			Digest:          digest,
		})
	}

	for _, cluster := range clusters {
		clusterPlan, err := planCluster(cfg, fedObject, policies, cluster)
		if err != nil {
			return nil, err
		}
		plan.Clusters = append(plan.Clusters, ClusterBinding{
			Name:   cluster.Name,
			Digest: digestCluster(cluster),
		})
		plan.ClusterPlans = append(plan.ClusterPlans, clusterPlan)
	}

	if cfg.mode == PlanModeStrict {
		if reasons := strictRejectionReasons(plan); len(reasons) > 0 {
			return plan, &StrictConflictError{Plan: plan, Reasons: reasons}
		}
	}

	return plan, nil
}

// planCluster mirrors parseOverrides for a single cluster while additionally recording provenance,
// pointer analysis and the candidate object.
func planCluster(
	cfg planConfig,
	fedObject fedcorev1a1.GenericFederatedObject,
	policies []fedcorev1a1.GenericOverridePolicy,
	cluster *fedcorev1a1.FederatedCluster,
) (ClusterPatchPlan, error) {
	clusterPlan := ClusterPatchPlan{
		Cluster:       cluster.Name,
		Patches:       make([]PlannedPatch, 0),
		FailedAtIndex: -1,
	}

	// A fresh helper per cluster, exactly like the lazily created helpers map of parseOverrides. It only
	// ever holds unstructured objects decoded from the template raw bytes, so the fedObject is untouched.
	helper, err := getHelpDataFromFedObj(fedObject)
	if err != nil {
		return clusterPlan, err
	}

	baseTemplate, err := fedObject.GetSpec().GetTemplateAsUnstructured()
	if err != nil {
		return clusterPlan, err
	}
	baseTemplateRaw, err := baseTemplate.MarshalJSON()
	if err != nil {
		return clusterPlan, err
	}

	globalIndex := 0
	aborted := false

	for _, policy := range policies {
		if aborted {
			break
		}
		spec := policy.GetSpec()
		for ruleIndex, rule := range spec.OverrideRules {
			matched, matchErr := isClusterMatched(rule.TargetClusters, cluster)
			if matchErr != nil {
				return clusterPlan, fmt.Errorf(
					"failed to match cluster %q to policy %q's overrideRules[%v]: %w",
					cluster.Name, policy.GetName(), ruleIndex, matchErr,
				)
			}
			if !matched {
				continue
			}

			rulePatches, ruleErr := planRulePatches(cfg, helper, policy, ruleIndex, rule.Overriders)
			if ruleErr != nil {
				return clusterPlan, fmt.Errorf(
					"failed to parse patches from policy %q's overrideRules[%v]: %w",
					policy.GetName(), ruleIndex, ruleErr,
				)
			}

			for i := range rulePatches {
				rulePatches[i].Index = globalIndex
				globalIndex++
				if globalIndex > cfg.limits.MaxPatches {
					return clusterPlan, fmt.Errorf(
						"%w: cluster %q produced more than %d patches",
						ErrPlanLimitExceeded, cluster.Name, cfg.limits.MaxPatches,
					)
				}
				clusterPlan.Patches = append(clusterPlan.Patches, rulePatches[i])

				if isFinalizerPatch(rulePatches[i].Patch.Path) {
					// Preserve the existing ignoreFinalizerPatches rule: finalizer patches are recorded
					// but never drive the candidate object.
					clusterPlan.Patches[len(clusterPlan.Patches)-1].Deferred = true
					continue
				}

				// Apply one patch at a time. RFC 6902 application is sequential, so this produces the
				// same bytes as parseOverrides' per-rule batch application while exposing the exact
				// failing index. On error the helper object stays at its pre-patch state.
				applyErr := ctrutil.ApplyJSONPatch(
					helper.sourceObj,
					fedcorev1a1.OverridePatches{rulePatches[i].Patch},
				)
				if applyErr != nil {
					clusterPlan.FailedAtIndex = rulePatches[i].Index
					clusterPlan.FailureReason = applyErr.Error()
					aborted = true
					break
				}
			}
			if aborted {
				break
			}
		}
	}

	// Static analysis runs over the full raw ordered list, including deferred patches, and replays the
	// document separately so that array relocations can be interpreted precisely.
	readSet, writeSet, conflicts, effectivePaths := analyzePatchConflicts(baseTemplateRaw, clusterPlan.Patches)
	clusterPlan.ReadPaths = sortedSetList(readSet)
	clusterPlan.WritePaths = sortedSetList(writeSet)
	clusterPlan.Conflicts = conflicts
	for i := range clusterPlan.Patches {
		if effective, ok := effectivePaths[clusterPlan.Patches[i].Index]; ok {
			clusterPlan.Patches[i].EffectivePath = effective
		}
		clusterPlan.Patches[i].Conflicts = conflictsForPatch(conflicts, clusterPlan.Patches[i].Index)
	}

	candidateRaw, err := helper.sourceObj.MarshalJSON()
	if err != nil {
		return clusterPlan, err
	}
	clusterPlan.candidateRaw = candidateRaw
	clusterPlan.CandidateBytes = int64(len(candidateRaw))
	if clusterPlan.CandidateBytes > cfg.limits.MaxCandidateBytes {
		return clusterPlan, fmt.Errorf(
			"%w: candidate object for cluster %q is %d bytes, limit is %d",
			ErrPlanLimitExceeded, cluster.Name, clusterPlan.CandidateBytes, cfg.limits.MaxCandidateBytes,
		)
	}

	redacted, didRedact := redactedCandidate(helper.sourceObj)
	clusterPlan.Candidate = redacted
	clusterPlan.Redacted = didRedact

	return clusterPlan, nil
}

// planRulePatches expands a single rule into ordered, source-annotated raw patches. The segment order
// and parser calls are identical to parsePatchesFromOverriders, which is what keeps the produced raw
// bytes compatible with the parseOverrides pipeline.
func planRulePatches(
	cfg planConfig,
	helper *helpData,
	policy fedcorev1a1.GenericOverridePolicy,
	ruleIndex int,
	overriders *fedcorev1a1.Overriders,
) ([]PlannedPatch, error) {
	planned := make([]PlannedPatch, 0)
	if overriders == nil {
		return planned, nil
	}

	baseSource := PatchSource{
		PolicyKind: policyKind(policy),
		PolicyKey:  policy.GetKey(),
		RuleIndex:  ruleIndex,
	}

	appendSegment := func(
		overriderType string,
		patches fedcorev1a1.OverridePatches,
		contributors func(path string) ([]int, string, error),
	) error {
		for _, patch := range patches {
			if err := validatePatchLimits(cfg.limits, patch); err != nil {
				return err
			}
			source := baseSource
			source.OverriderType = overriderType
			if contributors != nil {
				indexes, containerName, err := contributors(patch.Path)
				if err != nil {
					return err
				}
				source.OverriderIndexes = indexes
				source.ContainerName = containerName
			}
			planned = append(planned, PlannedPatch{Patch: patch, Source: source})
		}
		return nil
	}

	// Image
	imagePatches, err := parseImageOverriders(helper, overriders.Image)
	if err != nil {
		return nil, fmt.Errorf("failed to parse image overriders: %w", err)
	}
	if err = appendSegment(overriderTypeImage, imagePatches, func(path string) ([]int, string, error) {
		return imagePatchContributors(helper, overriders.Image, path)
	}); err != nil {
		return nil, err
	}

	// Command
	commandPatches, err := parseEntrypointOverriders(helper, overriders.Command, CommandTarget)
	if err != nil {
		return nil, fmt.Errorf("failed to parse command overriders: %w", err)
	}
	if err = appendSegment(overriderTypeCommand, commandPatches, func(path string) ([]int, string, error) {
		return entrypointPatchContributors(helper, overriders.Command, CommandTarget, path)
	}); err != nil {
		return nil, err
	}

	// Args
	argsPatches, err := parseEntrypointOverriders(helper, overriders.Args, ArgsTarget)
	if err != nil {
		return nil, fmt.Errorf("failed to parse args overriders: %w", err)
	}
	if err = appendSegment(overriderTypeArgs, argsPatches, func(path string) ([]int, string, error) {
		return entrypointPatchContributors(helper, overriders.Args, ArgsTarget, path)
	}); err != nil {
		return nil, err
	}

	// Envs
	envPatches, err := parseEnvOverriders(helper, overriders.Envs)
	if err != nil {
		return nil, fmt.Errorf("failed to parse env overriders: %w", err)
	}
	if err = appendSegment(overriderTypeEnvs, envPatches, func(path string) ([]int, string, error) {
		return envPatchContributors(helper, overriders.Envs, path)
	}); err != nil {
		return nil, err
	}

	// Annotations
	annotationPatches, err := parseStringMapOverriders(helper, overriders.Annotations, AnnotationsTarget)
	if err != nil {
		return nil, fmt.Errorf("failed to parse annotations overriders: %w", err)
	}
	if err = appendSegment(overriderTypeAnnotations, annotationPatches, func(string) ([]int, string, error) {
		return indexRange(len(overriders.Annotations)), "", nil
	}); err != nil {
		return nil, err
	}

	// Labels
	labelPatches, err := parseStringMapOverriders(helper, overriders.Labels, LabelsTarget)
	if err != nil {
		return nil, fmt.Errorf("failed to parse labels overriders: %w", err)
	}
	if err = appendSegment(overriderTypeLabels, labelPatches, func(string) ([]int, string, error) {
		return indexRange(len(overriders.Labels)), "", nil
	}); err != nil {
		return nil, err
	}

	// JsonPatch: output order matches the input overrider order one-to-one.
	jsonPatches, err := parseJSONPatchOverriders(overriders.JsonPatch)
	if err != nil {
		return nil, fmt.Errorf("failed to parse jsonpatch overriders: %w", err)
	}
	if err = appendSegment(overriderTypeJSONPatch, jsonPatches, nil); err != nil {
		return nil, err
	}
	for i := range planned {
		if planned[i].Source.OverriderType == overriderTypeJSONPatch {
			planned[i].Source.OverriderIndexes = []int{jsonPatchOrdinal(planned, i)}
		}
	}

	return planned, nil
}

// jsonPatchOrdinal returns the zero-based ordinal of a jsonpatch planned patch among the jsonpatch
// segment. The segment preserves input order, so the ordinal equals the overrider index.
func jsonPatchOrdinal(planned []PlannedPatch, i int) int {
	ordinal := 0
	for j := 0; j <= i; j++ {
		if planned[j].Source.OverriderType == overriderTypeJSONPatch {
			if j == i {
				return ordinal
			}
			ordinal++
		}
	}
	return ordinal
}

// imagePatchContributors resolves the image overriders that actually touch patchPath, expanding
// container names against the real pod spec instead of comparing raw overrider field names.
func imagePatchContributors(
	helper *helpData,
	imageOverriders []fedcorev1a1.ImageOverrider,
	patchPath string,
) ([]int, string, error) {
	indexes := make([]int, 0)
	for i := range imageOverriders {
		paths, err := imageOverriderPaths(helper, &imageOverriders[i])
		if err != nil {
			return nil, "", err
		}
		if paths.Has(patchPath) {
			indexes = append(indexes, i)
		}
	}
	containerName, _ := containerNameAtPath(helper, patchPath)
	return indexes, containerName, nil
}

// imageOverriderPaths computes the concrete image JSON pointers targeted by an image overrider using
// the same resolution rules as parsePatchesFromImagePath and parsePatchesFromWorkload.
func imageOverriderPaths(helper *helpData, imageOverrider *fedcorev1a1.ImageOverrider) (sets.Set[string], error) {
	paths := sets.New[string]()
	if imageOverrider.ImagePath != "" {
		paths.Insert(imageOverrider.ImagePath)
		return paths, nil
	}

	podSpec, err := podutil.GetResourcePodSpecFromUnstructuredObj(helper.sourceObj, helper.gvk)
	if err != nil {
		return nil, fmt.Errorf("failed to get podSpec from sourceObj: %w", err)
	}

	specifiedContainers := sets.New[string]()
	for _, name := range imageOverrider.ContainerNames {
		if name != "" {
			specifiedContainers.Insert(name)
		}
	}

	containerKinds := []string{InitContainers, Containers}
	for i, containers := range [][]corev1.Container{podSpec.InitContainers, podSpec.Containers} {
		for containerIndex := range containers {
			if len(imageOverrider.ContainerNames) == 0 || specifiedContainers.Has(containers[containerIndex].Name) {
				path, pathErr := generateTargetPathForPodSpec(helper.gvk, containerKinds[i], ImageTarget, containerIndex)
				if pathErr != nil {
					return nil, pathErr
				}
				paths.Insert(path)
			}
		}
	}
	return paths, nil
}

// entrypointPatchContributors resolves command/args overrider items targeting the real container
// behind patchPath.
func entrypointPatchContributors(
	helper *helpData,
	overriders []fedcorev1a1.EntrypointOverrider,
	target string,
	patchPath string,
) ([]int, string, error) {
	indexes := make([]int, 0)
	containerName := ""
	podSpec, err := podutil.GetResourcePodSpecFromUnstructuredObj(helper.sourceObj, helper.gvk)
	if err != nil {
		return nil, "", fmt.Errorf("failed to get podSpec from sourceObj: %w", err)
	}
	for i := range overriders {
		containerKind, containerIndex, container := lookForMatchedContainer(podSpec, overriders[i].ContainerName)
		if container == nil {
			continue
		}
		resolvedPath, pathErr := generateTargetPathForPodSpec(helper.gvk, containerKind, target, containerIndex)
		if pathErr != nil {
			return nil, "", pathErr
		}
		if resolvedPath == patchPath {
			indexes = append(indexes, i)
			containerName = container.Name
		}
	}
	return indexes, containerName, nil
}

// envPatchContributors resolves env overrider items targeting the real container behind patchPath.
func envPatchContributors(
	helper *helpData,
	overriders []fedcorev1a1.EnvOverrider,
	patchPath string,
) ([]int, string, error) {
	indexes := make([]int, 0)
	containerName := ""
	podSpec, err := podutil.GetResourcePodSpecFromUnstructuredObj(helper.sourceObj, helper.gvk)
	if err != nil {
		return nil, "", fmt.Errorf("failed to get podSpec from sourceObj: %w", err)
	}
	for i := range overriders {
		containerKind, containerIndex, container := lookForMatchedContainer(podSpec, overriders[i].ContainerName)
		if container == nil {
			continue
		}
		resolvedPath, pathErr := generateTargetPathForPodSpec(helper.gvk, containerKind, EnvTarget, containerIndex)
		if pathErr != nil {
			return nil, "", pathErr
		}
		if resolvedPath == patchPath {
			indexes = append(indexes, i)
			containerName = container.Name
		}
	}
	return indexes, containerName, nil
}

// containerNameAtPath resolves the container name for a container-scoped pointer such as
// /spec/template/spec/containers/0/image. It returns ok=false for non-container pointers.
func containerNameAtPath(helper *helpData, path string) (string, bool) {
	tokens, err := parseJSONPointer(path)
	if err != nil {
		return "", false
	}
	podSpec, err := podutil.GetResourcePodSpecFromUnstructuredObj(helper.sourceObj, helper.gvk)
	if err != nil {
		return "", false
	}
	for i := 0; i+1 < len(tokens); i++ {
		if tokens[i] != Containers && tokens[i] != InitContainers {
			continue
		}
		index, indexErr := parseArrayIndex(tokens[i+1])
		if indexErr != nil {
			return "", false
		}
		containers := podSpec.Containers
		if tokens[i] == InitContainers {
			containers = podSpec.InitContainers
		}
		if index >= 0 && index < len(containers) {
			return containers[index].Name, true
		}
	}
	return "", false
}

// Apply revalidates the plan against fresh inputs and, only when every binding and the re-planned raw
// patches and candidate objects match, returns the per-cluster raw patches. It performs no API writes
// itself and provides no cross-cluster transaction guarantee: each cluster entry is independently
// replayable.
func (plan *OverridePatchPlan) Apply(
	fedObject fedcorev1a1.GenericFederatedObject,
	policies []fedcorev1a1.GenericOverridePolicy,
	clusters []*fedcorev1a1.FederatedCluster,
) (map[string]fedcorev1a1.OverridePatches, error) {
	if fedObject == nil {
		return nil, fmt.Errorf("%w: fedObject is nil", ErrPlanBindingMismatch)
	}
	if fedObject.GetUID() != plan.FedObjectUID {
		return nil, fmt.Errorf("%w: uid changed from %q to %q", ErrPlanBindingMismatch, plan.FedObjectUID, fedObject.GetUID())
	}
	if fedObject.GetResourceVersion() != plan.FedObjectResourceVersion {
		return nil, fmt.Errorf(
			"%w: resourceVersion changed from %q to %q",
			ErrPlanBindingMismatch, plan.FedObjectResourceVersion, fedObject.GetResourceVersion(),
		)
	}
	if sha256Hex(fedObject.GetSpec().Template.Raw) != plan.TemplateDigest {
		return nil, fmt.Errorf("%w: template digest changed", ErrPlanBindingMismatch)
	}

	if len(policies) != len(plan.Policies) {
		return nil, fmt.Errorf("%w: matched policy count changed", ErrPlanBindingMismatch)
	}
	for i, policy := range policies {
		digest, err := digestPolicy(policy)
		if err != nil {
			return nil, err
		}
		binding := PolicyBinding{
			Kind:            policyKind(policy),
			Key:             policy.GetKey(),
			UID:             policy.GetUID(),
			ResourceVersion: policy.GetResourceVersion(),
			Digest:          digest,
		}
		if binding != plan.Policies[i] {
			return nil, fmt.Errorf("%w: policy %q snapshot changed", ErrPlanBindingMismatch, binding.Key)
		}
	}

	if len(clusters) != len(plan.Clusters) {
		return nil, fmt.Errorf("%w: cluster snapshot count changed", ErrPlanBindingMismatch)
	}
	for i, cluster := range clusters {
		binding := ClusterBinding{Name: cluster.Name, Digest: digestCluster(cluster)}
		if binding != plan.Clusters[i] {
			return nil, fmt.Errorf("%w: cluster %q snapshot changed", ErrPlanBindingMismatch, binding.Name)
		}
	}

	replanned, err := PlanOverrides(fedObject, policies, clusters, replayOptions(plan.config)...)
	if err != nil {
		var strictErr *StrictConflictError
		if !errors.As(err, &strictErr) {
			return nil, err
		}
		replanned = strictErr.Plan
	}

	if !equalPlans(plan, replanned) {
		return nil, fmt.Errorf("%w: re-planned patches or candidates differ", ErrPlanStale)
	}

	result := make(map[string]fedcorev1a1.OverridePatches, len(replanned.ClusterPlans))
	for _, clusterPlan := range replanned.ClusterPlans {
		if len(clusterPlan.Patches) == 0 {
			continue
		}
		patches := make(fedcorev1a1.OverridePatches, 0, len(clusterPlan.Patches))
		for _, plannedPatch := range clusterPlan.Patches {
			patches = append(patches, plannedPatch.Patch)
		}
		result[clusterPlan.Cluster] = patches
	}
	return result, nil
}

// RawCandidateBytes returns the unredacted candidate bytes for a cluster, mainly for in-package
// verification against the reference pipeline.
func (plan *OverridePatchPlan) RawCandidateBytes(clusterName string) ([]byte, bool) {
	for i := range plan.ClusterPlans {
		if plan.ClusterPlans[i].Cluster == clusterName {
			return plan.ClusterPlans[i].candidateRaw, true
		}
	}
	return nil, false
}

// equalPlans compares the replayable parts of two plans: ordered raw patches (including op, path,
// value and order), failure positions and candidate bytes. Metadata such as CreatedAt is ignored.
func equalPlans(a, b *OverridePatchPlan) bool {
	if len(a.ClusterPlans) != len(b.ClusterPlans) {
		return false
	}
	for i := range a.ClusterPlans {
		cpA, cpB := a.ClusterPlans[i], b.ClusterPlans[i]
		if cpA.Cluster != cpB.Cluster || cpA.FailedAtIndex != cpB.FailedAtIndex || cpA.CandidateBytes != cpB.CandidateBytes {
			return false
		}
		if len(cpA.Patches) != len(cpB.Patches) {
			return false
		}
		for j := range cpA.Patches {
			if !equalRawPatch(cpA.Patches[j].Patch, cpB.Patches[j].Patch) {
				return false
			}
			if cpA.Patches[j].Deferred != cpB.Patches[j].Deferred {
				return false
			}
		}
	}
	return true
}

func equalRawPatch(a, b fedcorev1a1.OverridePatch) bool {
	return a.Op == b.Op && a.Path == b.Path && string(a.Value.Raw) == string(b.Value.Raw)
}

func strictRejectionReasons(plan *OverridePatchPlan) []string {
	reasons := make([]string, 0)
	for _, clusterPlan := range plan.ClusterPlans {
		if clusterPlan.FailedAtIndex >= 0 {
			reasons = append(reasons, fmt.Sprintf(
				"cluster %q: patch %d failed to apply: %s",
				clusterPlan.Cluster, clusterPlan.FailedAtIndex, clusterPlan.FailureReason,
			))
		}
		for _, patch := range clusterPlan.Patches {
			for _, conflict := range patch.Conflicts {
				if conflict.Kind != ConflictWriteWrite && conflict.Kind != ConflictUnknown {
					continue
				}
				reasons = append(reasons, fmt.Sprintf(
					"cluster %q: %s at patch %d against earlier patch %d: %s",
					clusterPlan.Cluster, conflict.Kind, patch.Index, conflict.EarlierPatchIndex, conflict.Reason,
				))
			}
		}
	}
	sort.Strings(reasons)
	return reasons
}

func conflictsForPatch(conflicts []PatchConflict, patchIndex int) []PatchConflict {
	result := make([]PatchConflict, 0)
	for _, conflict := range conflicts {
		if conflict.EarlierPatchIndex == patchIndex {
			result = append(result, conflict)
		}
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

func validatePatchLimits(limits PlanLimits, patch fedcorev1a1.OverridePatch) error {
	if len(patch.Path) > limits.MaxPathBytes {
		return fmt.Errorf("%w: patch path is %d bytes, limit is %d", ErrPlanLimitExceeded, len(patch.Path), limits.MaxPathBytes)
	}
	if len(patch.Value.Raw) > limits.MaxPatchValueBytes {
		return fmt.Errorf(
			"%w: patch value at %q is %d bytes, limit is %d",
			ErrPlanLimitExceeded, patch.Path, len(patch.Value.Raw), limits.MaxPatchValueBytes,
		)
	}
	return nil
}

func isFinalizerPatch(path string) bool {
	return strings.HasPrefix(path, finalizersPathPrefix)
}

func indexRange(length int) []int {
	result := make([]int, 0, length)
	for i := 0; i < length; i++ {
		result = append(result, i)
	}
	return result
}

func replayOptions(cfg planConfig) []PlanOption {
	options := []PlanOption{WithPlanLimits(cfg.limits)}
	if cfg.mode == PlanModeStrict {
		options = append(options, WithStrictMode())
	}
	return options
}

func fedObjectKey(fedObject fedcorev1a1.GenericFederatedObject) string {
	if fedObject.GetNamespace() == "" {
		return fedObject.GetName()
	}
	return fedObject.GetNamespace() + "/" + fedObject.GetName()
}

func policyKind(policy fedcorev1a1.GenericOverridePolicy) string {
	switch policy.(type) {
	case *fedcorev1a1.ClusterOverridePolicy:
		return "ClusterOverridePolicy"
	case *fedcorev1a1.OverridePolicy:
		return "OverridePolicy"
	default:
		// GenericOverridePolicy is implemented by the two concrete types above; keep a safe fallback.
		return "GenericOverridePolicy"
	}
}

type policyDigestPayload struct {
	Kind            string                                 `json:"kind"`
	Key             string                                 `json:"key"`
	UID             types.UID                              `json:"uid"`
	ResourceVersion string                                 `json:"resourceVersion"`
	Spec            *fedcorev1a1.GenericOverridePolicySpec `json:"spec"`
}

func digestPolicy(policy fedcorev1a1.GenericOverridePolicy) (string, error) {
	payload := policyDigestPayload{
		Kind:            policyKind(policy),
		Key:             policy.GetKey(),
		UID:             policy.GetUID(),
		ResourceVersion: policy.GetResourceVersion(),
		Spec:            policy.GetSpec(),
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("failed to marshal policy digest payload: %w", err)
	}
	return sha256Hex(raw), nil
}

type clusterDigestPayload struct {
	Name   string            `json:"name"`
	Labels map[string]string `json:"labels"`
}

func digestCluster(cluster *fedcorev1a1.FederatedCluster) string {
	raw, err := json.Marshal(clusterDigestPayload{Name: cluster.Name, Labels: cluster.Labels})
	if err != nil {
		// All payload fields are plain JSON-compatible values; marshalling cannot realistically fail.
		return ""
	}
	return sha256Hex(raw)
}

func sha256Hex(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}
