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

package v1alpha1

import (
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:validation:Required
// +kubebuilder:resource:path=collectedstatuses,shortName=cs,singular=collectedstatus
// +kubebuilder:object:root=true

// CollectedStatus stores the collected fields of Kubernetes objects from member clusters, that are propagated by a
// FederatedObject.
type CollectedStatus struct {
	metav1.TypeMeta `json:",inline"`
	// Standard object's metadata.
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	GenericCollectedStatus `json:",inline"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true

// CollectedStatusList contains a list of CollectedStatuses.
type CollectedStatusList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CollectedStatus `json:"items"`
}

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:validation:Required
// +kubebuilder:resource:path=clustercollectedstatuses,shortName=ccs,singular=clustercollectedstatus,scope=Cluster
// +kubebuilder:object:root=true

// ClusterCollectedStatus stores the collected fields of Kubernetes objects from member clusters, that are propagated by
// a ClusterFederatedObject.
type ClusterCollectedStatus struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	GenericCollectedStatus `json:",inline"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClusterCollectedStatusList contains a list of ClusterCollectedStatuses.
type ClusterCollectedStatusList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ClusterCollectedStatus `json:"items"`
}

// GenericCollectedStatus contains the shared fields of CollectedStatus and ClusterCollectedStatus
type GenericCollectedStatus struct {
	// Clusters is the list of member clusters and collected fields for its propagated Kubernetes object.
	Clusters []CollectedFieldsWithCluster `json:"clusters"`

	// LastUpdateTime is the last time that a collection was performed.
	LastUpdateTime metav1.Time `json:"lastUpdateTime"`

	// Coverage is the optional per-cluster observation envelope for the explicit target cluster set.
	// It is only published when status collection coverage is explicitly enabled on the
	// FederatedTypeConfig. The legacy Clusters field above is left untouched when the envelope is
	// absent, so existing consumers remain fully compatible.
	// +optional
	Coverage *CollectedStatusCoverage `json:"coverage,omitempty"`
}

// CollectedFieldsWithCluster stores the collected fields of a Kubernetes object in a member cluster.
type CollectedFieldsWithCluster struct {
	// Cluster is the name of the member cluster.
	Cluster string `json:"cluster"`
	// CollectedFields is the the set of fields collected for the Kubernetes object.
	CollectedFields apiextensionsv1.JSON `json:"collectedFields"`
	// Error records any errors encountered while collecting fields from the cluster.
	// +optional
	Error string `json:"error,omitempty"`
}

// CoverageProtocolVersion is the version of the protocol shared by field collection and coverage
// publication. A collection and its coverage envelope always carry the same protocol version; an
// envelope produced by a different protocol version must be treated as stale.
const CoverageProtocolVersion = "v1alpha1"

// CoverageStrategy selects how a CollectedStatusCoverage envelope is allowed to be aggregated.
type CoverageStrategy string

const (
	// CoverageStrategyStrictAll requires every cluster in the explicit target set to be ready,
	// observed at the current propagation generation, with every configured field present, before
	// a complete aggregation can be published. Any missing observation invalidates the precise
	// aggregate instead of being silently treated as zero.
	CoverageStrategyStrictAll CoverageStrategy = "StrictAll"

	// CoverageStrategyPartial allows aggregation over the currently observable subset of the target
	// cluster set. The envelope is always marked incomplete and numeric totals are published as
	// ranges; missing replicas are never converted to zero to fabricate a precise total.
	CoverageStrategyPartial CoverageStrategy = "Partial"
)

// CoverageObservationState describes whether a single object or field was actually observed.
type CoverageObservationState string

const (
	// CoveragePresent means the object/field was observed and the value is populated. A real zero
	// value (e.g. status.replicas == 0) is reported as Present with Value set to 0, and must not be
	// confused with CoverageUnknown.
	CoveragePresent CoverageObservationState = "Present"
	// CoverageAbsent means observation succeeded but the object/field does not exist.
	CoverageAbsent CoverageObservationState = "Absent"
	// CoverageError means observation was attempted but failed (for example an unreachable member
	// cluster or a field traversal type error).
	CoverageError CoverageObservationState = "Error"
	// CoverageUnknown means no observation could be made yet: the cluster is not ready, the object
	// is not synced to the current propagation generation, or the observation is otherwise missing.
	CoverageUnknown CoverageObservationState = "Unknown"
)

// Machine-readable reasons supplementing ClusterCoverage.State.
const (
	// CoverageReasonClusterNotReady: the target cluster is not ready at collection time.
	CoverageReasonClusterNotReady = "ClusterNotReady"
	// CoverageReasonClusterUnreachable: the member cluster could not be reached during collection.
	CoverageReasonClusterUnreachable = "ClusterUnreachable"
	// CoverageReasonObjectAbsent: the propagated object does not (yet) exist in the member cluster.
	CoverageReasonObjectAbsent = "ObjectAbsent"
	// CoverageReasonStaleGeneration: the observed same-named object lags the current propagation
	// generation and cannot act as a receipt for it.
	CoverageReasonStaleGeneration = "StaleGeneration"
	// CoverageReasonObjectRebuilt: the observed object has a different identity than the propagated
	// receipt (for example recreated with a reset generation).
	CoverageReasonObjectRebuilt = "ObjectRebuilt"
	// CoverageReasonSecretFieldRefused: collection of a Secret field is forbidden.
	CoverageReasonSecretFieldRefused = "SecretFieldRefused"
	// CoverageReasonFieldLimitExceeded: the field was dropped because the configured field limit
	// was exceeded.
	CoverageReasonFieldLimitExceeded = "FieldLimitExceeded"
	// CoverageReasonClusterLimitExceeded: the cluster entry was dropped because the cluster limit
	// was exceeded.
	CoverageReasonClusterLimitExceeded = "ClusterLimitExceeded"
)

// Staleness reasons recorded on an envelope whose observations no longer describe the current
// desired state.
const (
	// CoverageStaleProtocol: the envelope was published under a different collection protocol.
	CoverageStaleProtocol = "ProtocolVersionChanged"
	// CoverageStaleConfig: the status collection configuration (fields or strategy) changed.
	CoverageStaleConfig = "CollectionConfigChanged"
	// CoverageStalePlacement: the explicit target cluster set changed.
	CoverageStalePlacement = "PlacementChanged"
	// CoverageStaleGeneration: propagation has not settled at the current FederatedObject generation.
	CoverageStaleGeneration = "PropagationGenerationOutOfDate"
)

// CollectedStatusCoverage is the optional observation envelope carried by a CollectedStatus or
// ClusterCollectedStatus object. It records exactly what was and was not observed for the explicit
// target cluster set, so status aggregators that declare coverage support can distinguish complete
// data from partial data instead of silently dropping unreachable clusters or treating missing
// replicas as zero.
type CollectedStatusCoverage struct {
	// ProtocolVersion is the field-collection/coverage-publication protocol version under which the
	// envelope was produced.
	ProtocolVersion string `json:"protocolVersion"`

	// Strategy is the aggregation strategy configured for this envelope.
	Strategy CoverageStrategy `json:"strategy"`

	// Complete reports whether the envelope covers the entire explicit target cluster set at the
	// current propagation generation, with every configured field present. It can only be true
	// under the StrictAll strategy; Partial envelopes are always incomplete.
	Complete bool `json:"complete"`

	// Stale reports that the envelope no longer matches the current collection protocol,
	// configuration, placement or propagation generation and must not be treated as a fresh receipt.
	// +optional
	Stale bool `json:"stale,omitempty"`
	// StaleReasons explains why the envelope is stale.
	// +optional
	StaleReasons []string `json:"staleReasons,omitempty"`

	// SourceGeneration is the generation of the source object recorded on the FederatedObject at
	// collection time.
	SourceGeneration int64 `json:"sourceGeneration,omitempty"`
	// FederatedGeneration is the metadata.generation of the FederatedObject at collection time.
	FederatedGeneration int64 `json:"federatedGeneration"`
	// SyncedGeneration is the FederatedObject status.syncedGeneration at collection time. It equals
	// FederatedGeneration only once propagation has fully settled.
	SyncedGeneration int64 `json:"syncedGeneration,omitempty"`

	// ConfigHash identifies the status collection configuration (collected fields plus coverage
	// strategy). A configuration change makes previously published envelopes stale.
	ConfigHash string `json:"configHash"`
	// PlacementHash identifies the explicit target cluster set. A placement change makes previously
	// published envelopes stale.
	PlacementHash string `json:"placementHash"`

	// TargetClusters is the size of the explicit target cluster set, including clusters that are
	// not ready or not yet synced; such clusters never silently disappear from the envelope.
	TargetClusters int `json:"targetClusters"`
	// ObservedClusters is the number of target clusters whose member object was observed at the
	// current propagation generation.
	ObservedClusters int `json:"observedClusters"`
	// Truncated reports that entries were dropped because a collection limit was exceeded.
	// +optional
	Truncated bool `json:"truncated,omitempty"`

	// Clusters contains one entry per target cluster (up to the configured cluster limit).
	Clusters []ClusterCoverage `json:"clusters"`
}

// ClusterCoverage records the observation receipt for one target cluster.
type ClusterCoverage struct {
	// Cluster is the name of the member cluster.
	Cluster string `json:"cluster"`
	// ClusterUID is the UID of the FederatedCluster object, used to detect cluster rebuilds.
	ClusterUID types.UID `json:"clusterUID,omitempty"`
	// Ready records whether the cluster was ready at collection time.
	Ready bool `json:"ready"`

	// MemberObjectUID is the UID of the propagated member object. It is empty when the object could
	// not be observed.
	// +optional
	MemberObjectUID types.UID `json:"memberObjectUID,omitempty"`
	// MemberObjectResourceVersion is the opaque resourceVersion of the member object. It is carried
	// as an identity token only and must never be parsed or compared numerically across clusters.
	// +optional
	MemberObjectResourceVersion string `json:"memberObjectResourceVersion,omitempty"`
	// MemberObjectGeneration is the metadata.generation of the observed member object.
	// +optional
	MemberObjectGeneration int64 `json:"memberObjectGeneration,omitempty"`
	// ExpectedGeneration is the member-object generation that a valid receipt is expected to carry,
	// derived from the per-cluster propagation status of the FederatedObject.
	// +optional
	ExpectedGeneration int64 `json:"expectedGeneration,omitempty"`

	// State is the object-level observation state.
	State CoverageObservationState `json:"state"`
	// Reason supplements State with a machine-readable explanation.
	// +optional
	Reason string `json:"reason,omitempty"`
	// Error records collection errors for the cluster, truncated to the configured error byte limit.
	// +optional
	Error string `json:"error,omitempty"`

	// Fields contains one entry per configured collection field.
	// +optional
	Fields []FieldCoverage `json:"fields,omitempty"`
}

// FieldCoverage records the observation state of one configured field in one target cluster.
type FieldCoverage struct {
	// Path is the dot-separated field path from the status collection configuration.
	Path string `json:"path"`
	// State is the field observation state. A real zero value is reported as Present with Value set
	// to 0, which must not be confused with Unknown.
	State CoverageObservationState `json:"state"`
	// Value carries the observed raw JSON value when State == Present.
	// +optional
	Value *apiextensionsv1.JSON `json:"value,omitempty"`
	// Error records field-level collection errors, truncated to the configured error byte limit.
	// +optional
	Error string `json:"error,omitempty"`
}
