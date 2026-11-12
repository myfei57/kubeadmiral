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

// Package statuscoverage contains the pure, informer-free logic that builds and consumes the
// optional CollectedStatusCoverage observation envelope. It deliberately distinguishes observed
// values (including real zeroes) from missing observations, so downstream aggregators never have
// to silently treat an unreachable cluster or a lagging object as zero.
package statuscoverage

import (
	"encoding/json"
	"sort"
	"strconv"
	"strings"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"

	fedcorev1a1 "github.com/kubewharf/kubeadmiral/pkg/apis/core/v1alpha1"
	"github.com/kubewharf/kubeadmiral/pkg/controllers/common"
)

const (
	// DefaultMaxClusters bounds the number of per-cluster entries in one envelope.
	DefaultMaxClusters = 200
	// DefaultMaxFields bounds the number of per-field entries in one cluster entry.
	DefaultMaxFields = 64
	// DefaultMaxErrorBytes bounds the byte length of every recorded error message.
	DefaultMaxErrorBytes = 1024

	// secretKind is the only Kind whose data fields are forbidden from collection.
	secretKind = common.SecretKind
)

// MemberObservation is everything the status controller was able to observe for one target member
// cluster at collection time. It is the raw input to the envelope builder and intentionally keeps
// lookup failures explicit instead of dropping the cluster.
type MemberObservation struct {
	// ClusterName is the name of the target member cluster.
	ClusterName string
	// ClusterUID is the UID of the FederatedCluster object. It may be empty when the cluster
	// itself cannot be observed.
	ClusterUID types.UID
	// Ready reports whether the cluster was ready at collection time.
	Ready bool

	// Object is the propagated member object. It is nil when the object is absent or could not be
	// retrieved.
	Object *unstructured.Unstructured
	// ObjectAbsent is true when the lookup succeeded but the propagated object does not exist.
	ObjectAbsent bool
	// ObjectError is set when the lookup itself failed (unreachable cluster, unsynced lister, ...).
	ObjectError error
}

// BuildOptions configures a single envelope build.
type BuildOptions struct {
	// Strategy is the configured aggregation strategy.
	Strategy fedcorev1a1.CoverageStrategy
	// Fields are the dot-separated field paths configured on the FederatedTypeConfig.
	Fields []string
	// SourceKind is the Kind of the propagated source object, used to refuse Secret fields.
	SourceKind string
	// Previous is the envelope last published for the same FederatedObject, used to mark staleness
	// transitions. It may be nil.
	Previous *fedcorev1a1.CollectedStatusCoverage

	// MaxClusters, MaxFields and MaxErrorBytes override the package defaults when positive.
	MaxClusters   int
	MaxFields     int
	MaxErrorBytes int
}

// TargetClusterNames returns the explicit target cluster set of a FederatedObject as a sorted,
// de-duplicated union of all controller placements. Clusters that are not ready or not yet synced
// remain part of this set and must not disappear from the coverage envelope.
func TargetClusterNames(fedObject fedcorev1a1.GenericFederatedObject) []string {
	if fedObject == nil {
		return nil
	}
	nameSet := make(map[string]struct{})
	for _, placement := range fedObject.GetSpec().Placements {
		for _, cluster := range placement.Placement {
			if cluster.Cluster != "" {
				nameSet[cluster.Cluster] = struct{}{}
			}
		}
	}
	names := make([]string, 0, len(nameSet))
	for name := range nameSet {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// Build constructs a CollectedStatusCoverage envelope for the explicit target cluster set of
// fedObject. The function is pure: it never performs I/O and never mutates its inputs. Every target
// cluster produces an entry, even when it is not ready, unreachable or not yet synced.
func Build(
	fedObject fedcorev1a1.GenericFederatedObject,
	observations []MemberObservation,
	opts BuildOptions,
) *fedcorev1a1.CollectedStatusCoverage {
	maxClusters, maxFields, maxErrorBytes := limits(opts)

	strategy := opts.Strategy
	if strategy != fedcorev1a1.CoverageStrategyPartial {
		strategy = fedcorev1a1.CoverageStrategyStrictAll
	}

	targets := TargetClusterNames(fedObject)
	observationMap := make(map[string]MemberObservation, len(observations))
	for _, observation := range observations {
		observationMap[observation.ClusterName] = observation
	}

	fedGeneration := fedObject.GetGeneration()
	syncedGeneration := fedObject.GetStatus().SyncedGeneration
	propagationSettled := syncedGeneration == fedGeneration

	expectedGenerations := make(map[string]int64, len(fedObject.GetStatus().Clusters))
	propagationOK := make(map[string]bool, len(fedObject.GetStatus().Clusters))
	for _, clusterStatus := range fedObject.GetStatus().Clusters {
		expectedGenerations[clusterStatus.Cluster] = clusterStatus.LastObservedGeneration
		propagationOK[clusterStatus.Cluster] = clusterStatus.Status == fedcorev1a1.ClusterPropagationOK
	}

	fields := normalizeFields(opts.Fields, maxFields)
	fieldTruncated := len(opts.Fields) > maxFields

	envelope := &fedcorev1a1.CollectedStatusCoverage{
		ProtocolVersion:     fedcorev1a1.CoverageProtocolVersion,
		Strategy:            strategy,
		SourceGeneration:    sourceGeneration(fedObject),
		FederatedGeneration: fedGeneration,
		SyncedGeneration:    syncedGeneration,
		ConfigHash:          ConfigHash(strategy, opts.Fields),
		PlacementHash:       PlacementHash(targets),
		TargetClusters:      len(targets),
		Clusters:            make([]fedcorev1a1.ClusterCoverage, 0, len(targets)),
	}

	clusterTruncated := len(targets) > maxClusters
	allPresent := true

	for _, clusterName := range targets {
		if len(envelope.Clusters) >= maxClusters {
			break
		}

		observation := observationMap[clusterName]
		entry := buildClusterCoverage(
			clusterName, observation, fields, opts.SourceKind,
			expectedGenerations, propagationOK, propagationSettled, maxErrorBytes,
		)
		if entry.State != fedcorev1a1.CoveragePresent {
			allPresent = false
		}
		for _, field := range entry.Fields {
			if field.State != fedcorev1a1.CoveragePresent {
				allPresent = false
			}
		}
		envelope.Clusters = append(envelope.Clusters, entry)
	}

	for _, entry := range envelope.Clusters {
		if entry.State == fedcorev1a1.CoveragePresent {
			envelope.ObservedClusters++
		}
	}

	envelope.Truncated = clusterTruncated || fieldTruncated
	if clusterTruncated || fieldTruncated {
		allPresent = false
	}

	if strategy == fedcorev1a1.CoverageStrategyStrictAll && propagationSettled && allPresent {
		envelope.Complete = true
	}

	markStaleness(envelope, opts.Previous, propagationSettled)
	return envelope
}

func buildClusterCoverage(
	clusterName string,
	observation MemberObservation,
	fields []string,
	sourceKind string,
	expectedGenerations map[string]int64,
	propagationOK map[string]bool,
	propagationSettled bool,
	maxErrorBytes int,
) fedcorev1a1.ClusterCoverage {
	entry := fedcorev1a1.ClusterCoverage{
		Cluster:    clusterName,
		ClusterUID: observation.ClusterUID,
		Ready:      observation.Ready,
		Fields:     make([]fedcorev1a1.FieldCoverage, 0, len(fields)),
	}

	// A cluster that is not ready (or has completely vanished from the joined set) stays in the
	// envelope with an explicit Unknown state; it is never silently dropped.
	if !observation.Ready {
		entry.State = fedcorev1a1.CoverageUnknown
		entry.Reason = fedcorev1a1.CoverageReasonClusterNotReady
		appendUnknownFields(&entry, fields)
		return entry
	}

	if observation.ObjectError != nil {
		entry.State = fedcorev1a1.CoverageError
		entry.Reason = fedcorev1a1.CoverageReasonClusterUnreachable
		entry.Error = truncateError(observation.ObjectError.Error(), maxErrorBytes)
		appendUnknownFields(&entry, fields)
		return entry
	}

	if observation.ObjectAbsent || observation.Object == nil {
		entry.State = fedcorev1a1.CoverageAbsent
		entry.Reason = fedcorev1a1.CoverageReasonObjectAbsent
		appendUnknownFields(&entry, fields)
		return entry
	}

	memberObject := observation.Object
	entry.MemberObjectUID = memberObject.GetUID()
	// resourceVersion is recorded as an opaque identity token only; it is never parsed or compared
	// numerically, and especially never compared across different clusters.
	entry.MemberObjectResourceVersion = memberObject.GetResourceVersion()
	entry.MemberObjectGeneration = memberObject.GetGeneration()
	entry.ExpectedGeneration = expectedGenerations[clusterName]

	if !isCurrentReceipt(memberObject.GetGeneration(), entry.ExpectedGeneration,
		propagationOK[clusterName], propagationSettled) {
		entry.State = fedcorev1a1.CoverageUnknown
		if entry.ExpectedGeneration > 0 && memberObject.GetGeneration() < entry.ExpectedGeneration {
			// The same-named object is younger than the last propagated receipt: it was rebuilt and
			// cannot acknowledge the current propagation generation.
			entry.Reason = fedcorev1a1.CoverageReasonObjectRebuilt
		} else {
			entry.Reason = fedcorev1a1.CoverageReasonStaleGeneration
		}
		appendUnknownFields(&entry, fields)
		return entry
	}

	entry.State = fedcorev1a1.CoveragePresent
	for _, field := range fields {
		entry.Fields = append(entry.Fields, collectField(memberObject, field, sourceKind, maxErrorBytes))
	}
	return entry
}

// isCurrentReceipt decides whether an observed same-named member object may serve as a receipt for
// the current propagation generation. A lagging or rebuilt object is rejected even though it lives
// in a ready cluster.
func isCurrentReceipt(memberGeneration, expectedGeneration int64, clusterPropagationOK, propagationSettled bool) bool {
	if !propagationSettled || !clusterPropagationOK {
		return false
	}
	return memberGeneration == expectedGeneration
}

func collectField(
	memberObject *unstructured.Unstructured,
	field, sourceKind string,
	maxErrorBytes int,
) fedcorev1a1.FieldCoverage {
	coverage := fedcorev1a1.FieldCoverage{Path: field}

	if isForbiddenSecretField(sourceKind, field) {
		coverage.State = fedcorev1a1.CoverageError
		coverage.Error = fedcorev1a1.CoverageReasonSecretFieldRefused
		return coverage
	}

	value, found, err := unstructured.NestedFieldCopy(memberObject.Object, strings.Split(field, ".")...)
	if err != nil {
		coverage.State = fedcorev1a1.CoverageError
		coverage.Error = truncateError(err.Error(), maxErrorBytes)
		return coverage
	}
	if !found {
		coverage.State = fedcorev1a1.CoverageAbsent
		return coverage
	}

	raw, err := json.Marshal(value)
	if err != nil {
		coverage.State = fedcorev1a1.CoverageError
		coverage.Error = truncateError(err.Error(), maxErrorBytes)
		return coverage
	}

	// Present with an explicit value. A real zero (e.g. status.replicas == 0) is marshalled as `0`
	// and therefore stays distinguishable from an unobserved field.
	coverage.State = fedcorev1a1.CoveragePresent
	coverage.Value = &apiextensionsv1.JSON{Raw: raw}
	return coverage
}

func appendUnknownFields(entry *fedcorev1a1.ClusterCoverage, fields []string) {
	for _, field := range fields {
		entry.Fields = append(entry.Fields, fedcorev1a1.FieldCoverage{
			Path:  field,
			State: fedcorev1a1.CoverageUnknown,
		})
	}
}

// isForbiddenSecretField reports whether a configured field path would expose Secret material.
// Only the data and stringData subtrees of a Secret are forbidden; metadata and status remain
// collectable.
func isForbiddenSecretField(sourceKind, field string) bool {
	if sourceKind != secretKind {
		return false
	}
	segments := strings.Split(field, ".")
	return len(segments) > 0 && (segments[0] == "data" || segments[0] == "stringData")
}

func markStaleness(
	envelope *fedcorev1a1.CollectedStatusCoverage,
	previous *fedcorev1a1.CollectedStatusCoverage,
	propagationSettled bool,
) {
	addReason := func(reason string) {
		envelope.Stale = true
		for _, existing := range envelope.StaleReasons {
			if existing == reason {
				return
			}
		}
		envelope.StaleReasons = append(envelope.StaleReasons, reason)
	}

	if !propagationSettled {
		addReason(fedcorev1a1.CoverageStaleGeneration)
	}
	for _, entry := range envelope.Clusters {
		if entry.Reason == fedcorev1a1.CoverageReasonStaleGeneration ||
			entry.Reason == fedcorev1a1.CoverageReasonObjectRebuilt {
			addReason(fedcorev1a1.CoverageStaleGeneration)
			break
		}
	}

	if previous == nil {
		return
	}
	if previous.ProtocolVersion != envelope.ProtocolVersion {
		addReason(fedcorev1a1.CoverageStaleProtocol)
	}
	if previous.ConfigHash != envelope.ConfigHash {
		addReason(fedcorev1a1.CoverageStaleConfig)
	}
	if previous.PlacementHash != envelope.PlacementHash {
		addReason(fedcorev1a1.CoverageStalePlacement)
	}
}

func normalizeFields(configured []string, maxFields int) []string {
	seen := make(map[string]struct{}, len(configured))
	fields := make([]string, 0, len(configured))
	for _, field := range configured {
		if field == "" {
			continue
		}
		if _, ok := seen[field]; ok {
			continue
		}
		seen[field] = struct{}{}
		if len(fields) >= maxFields {
			break
		}
		fields = append(fields, field)
	}
	return fields
}

func sourceGeneration(fedObject fedcorev1a1.GenericFederatedObject) int64 {
	raw, ok := fedObject.GetAnnotations()[common.SourceGenerationAnnotation]
	if !ok {
		return 0
	}
	generation, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0
	}
	return generation
}

func truncateError(message string, maxErrorBytes int) string {
	if len(message) <= maxErrorBytes {
		return message
	}
	return message[:maxErrorBytes]
}

func limits(opts BuildOptions) (maxClusters, maxFields, maxErrorBytes int) {
	maxClusters, maxFields, maxErrorBytes = opts.MaxClusters, opts.MaxFields, opts.MaxErrorBytes
	if maxClusters <= 0 {
		maxClusters = DefaultMaxClusters
	}
	if maxFields <= 0 {
		maxFields = DefaultMaxFields
	}
	if maxErrorBytes <= 0 {
		maxErrorBytes = DefaultMaxErrorBytes
	}
	return maxClusters, maxFields, maxErrorBytes
}
