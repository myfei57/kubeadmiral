/*
Copyright 2024 The KubeAdmiral Authors.

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

// Package propagationbarrier provides the PropagationBarrier token and the
// WaitForPropagation API.
//
// A PropagationBarrier is an immutable, signed token that pins one specific
// propagation input identity: the federated object UID and generation, the
// template and override digests and an explicit set of target member clusters
// identified by cluster UID. WaitForPropagation waits until every target has
// both a managed dispatch receipt (the persisted PropagatedVersion entry) and
// a matching member-cluster observation for the same input identity.
//
// The barrier deliberately does NOT rely on a global Synced condition or on
// whichever CollectedStatus happens to be newest. A token becomes Stale when
// the federated object is replaced, its generation or digests change, the
// placement no longer contains a target, or the target cluster is rebuilt
// (same name, different UID). resourceVersions are never ordered or compared
// across different clusters.
//
// Persistent observations (informer state and direct API reads) are the sole
// source of truth; watch events are only used to wake up waiting goroutines and
// may be lost safely because the manager periodically re-queries. Waiting never
// performs a write and never retries business side effects on its own.
package propagationbarrier

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

// ObjectRef identifies a FederatedObject/ClusterFederatedObject and its
// same-named PropagatedVersion/ClusterPropagatedVersion. An empty Namespace
// denotes the cluster-scoped variants.
type ObjectRef struct {
	Namespace string `json:"namespace,omitempty"`
	Name      string `json:"name"`
}

// String renders the reference as "namespace/name" or "name".
func (r ObjectRef) String() string {
	if r.Namespace == "" {
		return r.Name
	}
	return r.Namespace + "/" + r.Name
}

// PropagationTarget pins a single member cluster. ClusterName is retained for
// lookups but ClusterUID is the authoritative identity: deleting and
// recreating a cluster with the same name produces a new UID and invalidates
// any token targeting the old instance.
type PropagationTarget struct {
	// ClusterName is the current name of the target cluster.
	ClusterName string `json:"clusterName"`
	// ClusterUID pins the exact cluster instance the token was issued for.
	ClusterUID types.UID `json:"clusterUID"`
}

// PropagationBarrier is an immutable token that pins the exact input identity
// of one propagation operation. Tokens are signed and encoded as opaque
// strings; callers must not inspect or construct the encoding themselves.
type PropagationBarrier struct {
	// Version is the token format version.
	Version int `json:"version"`
	// TokenID is the opaque, random identifier of the token. It is also the
	// key under which the manager keeps its server-side (in-memory) state.
	TokenID string `json:"tokenID"`

	// ObjectRef identifies the FederatedObject or ClusterFederatedObject.
	ObjectRef ObjectRef `json:"objectRef"`
	// FederatedGVK is the GroupVersionKind of the federated object itself
	// (FederatedObject or ClusterFederatedObject).
	FederatedGVK schema.GroupVersionKind `json:"federatedGvk"`
	// SourceGVK is the GroupVersionKind of the workload being propagated.
	SourceGVK schema.GroupVersionKind `json:"sourceGvk"`

	// FederatedUID pins the federated object instance. A same-named
	// replacement object carries a different UID and makes the token Stale.
	FederatedUID types.UID `json:"federatedUID"`
	// Generation pins the federated object generation the token was issued for.
	Generation int64 `json:"generation"`

	// TemplateDigest is the digest of the propagated template at issue time.
	TemplateDigest string `json:"templateDigest"`
	// OverrideDigest is the digest of the full override set at issue time.
	OverrideDigest string `json:"overrideDigest"`

	// Targets is the explicit, UID-pinned set of member clusters that must all
	// confirm the pinned input identity before WaitForPropagation completes.
	Targets []PropagationTarget `json:"targets"`

	// IssuedAt and ExpiresAt bound the lifetime of the token.
	IssuedAt  time.Time `json:"issuedAt"`
	ExpiresAt time.Time `json:"expiresAt"`
}

// Expired reports whether the token has outlived its TTL at the given time.
func (b *PropagationBarrier) Expired(now time.Time) bool {
	return !now.Before(b.ExpiresAt)
}

// TargetState describes the per-target outcome of one barrier evaluation.
// The four non-complete conditions are deliberately distinct so callers can
// tell missing dispatch, explicit errors and pending member observation apart.
type TargetState string

const (
	// TargetComplete means both the managed dispatch receipt and the member
	// observation match the pinned input identity for the target.
	TargetComplete TargetState = "Complete"
	// TargetMissing means no dispatch receipt for the pinned input identity
	// has been persisted yet (no PropagatedVersion, stale digests or no entry
	// for the cluster).
	TargetMissing TargetState = "Missing"
	// TargetUnobserved means a matching dispatch receipt exists but the
	// member object has not yet been observed in the required state, e.g.
	// informer propagation lag or the member still running an old generation.
	TargetUnobserved TargetState = "Unobserved"
	// TargetError means an explicit error was observed for the target, such as
	// a failed propagation status or an unmanaged/unreadable member object.
	TargetError TargetState = "Error"
	// TargetStale means the token can never be satisfied because the pinned
	// input identity no longer exists (object replacement, generation/digest
	// change, placement change or cluster rebuild/removal).
	TargetStale TargetState = "Stale"
)

// Stable reason identifiers attached to TargetResult.
const (
	ReasonTokenExpired             = "TokenExpired"
	ReasonFedObjectMissing         = "FedObjectMissing"
	ReasonFedObjectUIDMismatch     = "FedObjectUIDMismatch"
	ReasonGenerationMismatch       = "GenerationMismatch"
	ReasonTemplateDigestMismatch   = "TemplateDigestMismatch"
	ReasonOverrideDigestMismatch   = "OverrideDigestMismatch"
	ReasonClusterMissing           = "ClusterMissing"
	ReasonClusterUIDMismatch       = "ClusterUIDMismatch"
	ReasonPlacementChanged         = "PlacementChanged"
	ReasonNoDispatchReceipt        = "NoDispatchReceipt"
	ReasonNoClusterReceipt         = "NoClusterReceipt"
	ReasonPropagationError         = "PropagationError"
	ReasonMemberRetrievalFailed    = "MemberRetrievalFailed"
	ReasonMemberObjectNotObserved  = "MemberObjectNotObserved"
	ReasonMemberObjectNotManaged   = "MemberObjectNotManaged"
	ReasonMemberVersionMismatch    = "MemberVersionMismatch"
	ReasonMemberGenerationMismatch = "MemberGenerationMismatch"
	ReasonPendingCreate            = "PendingCreate"
	ReasonRemovalPending           = "RemovalPending"
)

// WaitState is the aggregate outcome of a WaitForPropagation call.
type WaitState string

const (
	// WaitCompleted means every target reached TargetComplete.
	WaitCompleted WaitState = "Completed"
	// WaitStale means at least one target proved the token unsatisfiable.
	WaitStale WaitState = "Stale"
	// WaitTimeout means the token TTL elapsed before all targets completed.
	WaitTimeout WaitState = "Timeout"
	// WaitCanceled means the caller context ended or the manager shut down.
	WaitCanceled WaitState = "Canceled"
)

// TargetResult is the evaluated state of one target at a given time. It only
// attests that the pinned input identity was propagated to the target; it does
// not imply the member workload is Healthy and it says nothing about future
// modifications of the object.
type TargetResult struct {
	ClusterName string      `json:"clusterName"`
	ClusterUID  types.UID   `json:"clusterUID"`
	State       TargetState `json:"state"`
	// Reason is a stable machine-readable identifier.
	Reason string `json:"reason,omitempty"`
	// Message is a human-readable explanation.
	Message string `json:"message,omitempty"`
}

// WaitOutcome is returned by WaitForPropagation.
type WaitOutcome struct {
	// TokenID identifies the token the outcome belongs to.
	TokenID string `json:"tokenID"`
	// State is the aggregate wait state.
	State WaitState `json:"state"`
	// Results contains one entry per token target in token order.
	Results []TargetResult `json:"results"`
	// ObservedAt is the time the final evaluation was performed.
	ObservedAt metav1.Time `json:"observedAt"`
}

// AllComplete reports whether every target result is TargetComplete.
func (o *WaitOutcome) AllComplete() bool {
	for i := range o.Results {
		if o.Results[i].State != TargetComplete {
			return false
		}
	}
	return len(o.Results) > 0
}

// HasStale reports whether any target proved the token Stale.
func (o *WaitOutcome) HasStale() bool {
	for i := range o.Results {
		if o.Results[i].State == TargetStale {
			return true
		}
	}
	return false
}

// Limits bounds the resources a single manager and its tokens may consume.
// Zero fields are replaced with defaults from DefaultLimits.
type Limits struct {
	// MaxTargets is the maximum number of explicit targets per token.
	MaxTargets int
	// DefaultTTL is assigned to tokens that do not specify a TTL.
	DefaultTTL time.Duration
	// MaxTTL caps the TTL a token may request.
	MaxTTL time.Duration
	// MaxWaitersPerToken bounds the number of concurrently waiting callers
	// attached to one token.
	MaxWaitersPerToken int
	// MaxTokens bounds the number of live (non-expired) tokens in the manager.
	MaxTokens int
	// MaxTokenPayloadBytes bounds the encoded size of a single token.
	MaxTokenPayloadBytes int
	// MaxTotalQueueBytes bounds the approximate memory retained for all tokens
	// and their waiters, including coalesced wake slots.
	MaxTotalQueueBytes int
	// RequeryInterval is the periodic re-query interval that makes waits
	// resilient to lost watch events and informer reconnects.
	RequeryInterval time.Duration
	// SweepInterval controls how often expired tokens are garbage collected.
	SweepInterval time.Duration
	// wakeBufferSlots is the capacity of each token's coalesced wake channel.
	wakeBufferSlots int
	// perWaiterOverheadBytes is the bookkeeping estimate per waiter.
	perWaiterOverheadBytes int
	// tokenOverheadBytes is the fixed bookkeeping estimate per token.
	tokenOverheadBytes int
}

// DefaultLimits returns the production default limits. They are conservative on
// purpose: the barrier stores only bounded in-memory state and performs no
// writes, so resource usage stays predictable under caller pressure.
func DefaultLimits() Limits {
	return Limits{
		MaxTargets:             64,
		DefaultTTL:             10 * time.Minute,
		MaxTTL:                 30 * time.Minute,
		MaxWaitersPerToken:     128,
		MaxTokens:              10000,
		MaxTokenPayloadBytes:   16 * 1024,
		MaxTotalQueueBytes:     16 * 1024 * 1024,
		RequeryInterval:        5 * time.Second,
		SweepInterval:          30 * time.Second,
		wakeBufferSlots:        1,
		perWaiterOverheadBytes: 512,
		tokenOverheadBytes:     1024,
	}
}

func (l *Limits) withDefaults() Limits {
	d := DefaultLimits()
	if l.MaxTargets <= 0 {
		l.MaxTargets = d.MaxTargets
	}
	if l.DefaultTTL <= 0 {
		l.DefaultTTL = d.DefaultTTL
	}
	if l.MaxTTL <= 0 {
		l.MaxTTL = d.MaxTTL
	}
	if l.MaxTTL < l.DefaultTTL {
		l.DefaultTTL = l.MaxTTL
	}
	if l.MaxWaitersPerToken <= 0 {
		l.MaxWaitersPerToken = d.MaxWaitersPerToken
	}
	if l.MaxTokens <= 0 {
		l.MaxTokens = d.MaxTokens
	}
	if l.MaxTokenPayloadBytes <= 0 {
		l.MaxTokenPayloadBytes = d.MaxTokenPayloadBytes
	}
	if l.MaxTotalQueueBytes <= 0 {
		l.MaxTotalQueueBytes = d.MaxTotalQueueBytes
	}
	if l.RequeryInterval <= 0 {
		l.RequeryInterval = d.RequeryInterval
	}
	if l.SweepInterval <= 0 {
		l.SweepInterval = d.SweepInterval
	}
	if l.wakeBufferSlots <= 0 {
		l.wakeBufferSlots = d.wakeBufferSlots
	}
	if l.perWaiterOverheadBytes <= 0 {
		l.perWaiterOverheadBytes = d.perWaiterOverheadBytes
	}
	if l.tokenOverheadBytes <= 0 {
		l.tokenOverheadBytes = d.tokenOverheadBytes
	}
	return *l
}
