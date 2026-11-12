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

package propagationbarrier

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	fedcorev1a1 "github.com/kubewharf/kubeadmiral/pkg/apis/core/v1alpha1"
	"github.com/kubewharf/kubeadmiral/pkg/controllers/common"
	"github.com/kubewharf/kubeadmiral/pkg/controllers/sync/propagatedversion"
	"github.com/kubewharf/kubeadmiral/pkg/util/managedlabel"
)

// isNilFedObject treats both a nil interface and a typed-nil interface
// (e.g. a nil *FederatedObject wrapped in GenericFederatedObject) as absent.
func isNilFedObject(obj fedcorev1a1.GenericFederatedObject) bool {
	if obj == nil {
		return true
	}
	value := reflect.ValueOf(obj)
	return value.Kind() == reflect.Ptr && value.IsNil()
}

// targetObservation holds the persistent observations relevant to one target.
type targetObservation struct {
	cluster     *fedcorev1a1.FederatedCluster
	memberObj   *unstructured.Unstructured
	memberFound bool
	memberErr   error
}

// observationSnapshot is one consistent read batch for a token. All waiters
// attached to the same token share a single snapshot per evaluation, so N
// waiters cost the same API/informer reads as one.
type observationSnapshot struct {
	fedObject fedcorev1a1.GenericFederatedObject
	pvStatus  *fedcorev1a1.PropagatedVersionStatus
	targets   map[string]*targetObservation
}

// takeSnapshot performs the (strictly read-only) observations needed to
// evaluate every target of the barrier exactly once.
func takeSnapshot(
	ctx context.Context,
	reader StateReader,
	observer MemberObjectObserver,
	barrier *PropagationBarrier,
) (*observationSnapshot, error) {
	snap := &observationSnapshot{targets: make(map[string]*targetObservation, len(barrier.Targets))}

	fedObject, err := reader.GetFederatedObject(ctx, barrier.ObjectRef)
	if err != nil {
		return nil, fmt.Errorf("failed to read federated object %s: %w", barrier.ObjectRef.String(), err)
	}
	snap.fedObject = fedObject

	pvStatus, err := reader.GetPropagatedVersion(ctx, barrier.ObjectRef)
	if err != nil {
		return nil, fmt.Errorf("failed to read propagated version %s: %w", barrier.ObjectRef.String(), err)
	}
	snap.pvStatus = pvStatus

	for _, target := range barrier.Targets {
		observation := &targetObservation{}

		cluster, err := reader.GetFederatedCluster(ctx, target.ClusterName)
		if err != nil {
			// Cluster read failures are surfaced as an explicit, non-terminal
			// target error: the next re-query can recover without any write.
			observation.memberErr = fmt.Errorf("failed to read federated cluster %q: %w", target.ClusterName, err)
		} else {
			observation.cluster = cluster
			if cluster != nil && fedObject != nil {
				targetName := common.NewQualifiedName(fedObject)
				if template, tplErr := fedObject.GetSpec().GetTemplateAsUnstructured(); tplErr == nil {
					targetName = common.QualifiedName{
						Namespace: template.GetNamespace(),
						Name:      template.GetName(),
					}
				}
				obj, found, objErr := observer.ObserveMemberObject(
					ctx, target.ClusterName, barrier.SourceGVK, targetName,
				)
				observation.memberObj = obj
				observation.memberFound = found
				observation.memberErr = objErr
			}
		}

		snap.targets[target.ClusterName] = observation
	}

	return snap, nil
}

// evaluate classifies every token target against one snapshot. It never
// mutates the snapshot or any API object.
func evaluate(barrier *PropagationBarrier, snap *observationSnapshot, now time.Time) []TargetResult {
	results := make([]TargetResult, 0, len(barrier.Targets))
	for _, target := range barrier.Targets {
		results = append(results, evaluateTarget(barrier, target, snap, now))
	}
	return results
}

// evaluateTarget implements the identity verification state machine for one
// target. Stale checks come first because a Stale token can never be satisfied
// regardless of what receipts or member objects show.
func evaluateTarget(
	barrier *PropagationBarrier,
	target PropagationTarget,
	snap *observationSnapshot,
	now time.Time,
) TargetResult {
	result := TargetResult{
		ClusterName: target.ClusterName,
		ClusterUID:  target.ClusterUID,
	}

	if barrier.Expired(now) {
		return stale(result, ReasonTokenExpired, "token TTL elapsed")
	}

	// --- Federated object identity: UID, generation, input digests ---

	if isNilFedObject(snap.fedObject) {
		return stale(result, ReasonFedObjectMissing,
			fmt.Sprintf("federated object %s no longer exists", barrier.ObjectRef.String()))
	}
	if snap.fedObject.GetUID() != barrier.FederatedUID {
		return stale(result, ReasonFedObjectUIDMismatch,
			fmt.Sprintf("federated object UID changed from %s to %s (same-name replacement)",
				barrier.FederatedUID, snap.fedObject.GetUID()))
	}
	if snap.fedObject.GetGeneration() != barrier.Generation {
		return stale(result, ReasonGenerationMismatch,
			fmt.Sprintf("federated object generation changed from %d to %d",
				barrier.Generation, snap.fedObject.GetGeneration()))
	}

	template, err := snap.fedObject.GetSpec().GetTemplateAsUnstructured()
	if err != nil {
		return errored(result, ReasonPropagationError, fmt.Sprintf("failed to read current template: %v", err))
	}
	currentTemplateDigest, err := DigestTemplate(template)
	if err != nil {
		return errored(result, ReasonPropagationError, fmt.Sprintf("failed to hash current template: %v", err))
	}
	if currentTemplateDigest != barrier.TemplateDigest {
		return stale(result, ReasonTemplateDigestMismatch,
			"template was modified after the barrier token was issued")
	}
	currentOverrideDigest, err := DigestOverrides(snap.fedObject.GetSpec().Overrides)
	if err != nil {
		return errored(result, ReasonPropagationError, fmt.Sprintf("failed to hash current overrides: %v", err))
	}
	if currentOverrideDigest != barrier.OverrideDigest {
		return stale(result, ReasonOverrideDigestMismatch,
			"overrides were modified after the barrier token was issued")
	}

	// --- Cluster identity: existence, UID, current placement ---

	observation := snap.targets[target.ClusterName]
	if observation == nil || observation.cluster == nil {
		if observation != nil && observation.memberErr != nil && observation.cluster == nil {
			return errored(result, ReasonMemberRetrievalFailed, observation.memberErr.Error())
		}
		return stale(result, ReasonClusterMissing,
			fmt.Sprintf("federated cluster %q no longer exists", target.ClusterName))
	}
	if observation.cluster.GetUID() != target.ClusterUID {
		// Same cluster name, different UID: the cluster was rebuilt. Any old
		// receipt or member object belongs to a different cluster instance.
		return stale(result, ReasonClusterUIDMismatch,
			fmt.Sprintf("cluster %q UID changed from %s to %s (cluster rebuilt)",
				target.ClusterName, target.ClusterUID, observation.cluster.GetUID()))
	}
	if !snap.fedObject.GetSpec().GetPlacementUnion().Has(target.ClusterName) {
		return stale(result, ReasonPlacementChanged,
			fmt.Sprintf("cluster %q is no longer in the placement of %s",
				target.ClusterName, barrier.ObjectRef.String()))
	}

	// --- Managed dispatch receipt: persisted PropagatedVersion ---

	receiptVersion, receiptState := dispatchReceiptVersion(barrier, snap.pvStatus, target.ClusterName, result)
	if receiptState != nil {
		return *receiptState
	}

	// --- Explicit propagation error recorded for this target ---

	if propagationResult, isTerminal := recordedPropagationStatus(snap.fedObject, target.ClusterName, result); isTerminal {
		return propagationResult
	}

	// --- Member observation of the same input identity ---

	if observation.memberErr != nil {
		return errored(result, ReasonMemberRetrievalFailed, observation.memberErr.Error())
	}
	if !observation.memberFound || observation.memberObj == nil {
		// The dispatch receipt exists but the member object has not been
		// observed yet, e.g. informer cache lag after a successful dispatch.
		return unobserved(result, ReasonMemberObjectNotObserved,
			fmt.Sprintf("managed object not yet observed in cluster %q", target.ClusterName))
	}
	if !managedlabel.HasManagedLabel(observation.memberObj) {
		return errored(result, ReasonMemberObjectNotManaged,
			fmt.Sprintf("object in cluster %q is not labeled as managed by kubeadmiral", target.ClusterName))
	}

	// The receipt version and the observed member version are only ever
	// compared within the same cluster. resourceVersions are never ordered or
	// compared across different clusters.
	observedVersion := propagatedversion.ObjectVersion(observation.memberObj)
	if observedVersion != receiptVersion {
		return unobserved(result, ReasonMemberVersionMismatch,
			fmt.Sprintf("member object version %q does not match dispatch receipt %q in cluster %q",
				observedVersion, receiptVersion, target.ClusterName))
	}
	if sourceGenerationMismatch(template, observation.memberObj) {
		return unobserved(result, ReasonMemberGenerationMismatch,
			fmt.Sprintf("member object in cluster %q still carries an old source generation", target.ClusterName))
	}

	result.State = TargetComplete
	return result
}

// dispatchReceiptVersion extracts the cluster's receipt version from the
// persisted PropagatedVersion status. A non-nil TargetResult means evaluation
// must short-circuit (Missing conditions); nil means a usable receipt exists.
func dispatchReceiptVersion(
	barrier *PropagationBarrier,
	pvStatus *fedcorev1a1.PropagatedVersionStatus,
	clusterName string,
	result TargetResult,
) (string, *TargetResult) {
	if pvStatus == nil {
		return "", ptr(missing(result, ReasonNoDispatchReceipt,
			"no propagated version resource exists yet, nothing has been dispatched"))
	}
	// The persisted version resource must carry the exact input identity the
	// token pinned. An old-generation receipt (different digests) proves
	// nothing about the current input and must not satisfy the token.
	if pvStatus.TemplateVersion != barrier.TemplateDigest ||
		pvStatus.OverrideVersion != barrier.OverrideDigest {
		return "", ptr(missing(result, ReasonNoDispatchReceipt,
			"propagated version does not yet record the pinned template/override digests"))
	}
	for _, clusterVersion := range pvStatus.ClusterVersions {
		if clusterVersion.ClusterName == clusterName {
			if clusterVersion.Version == "" {
				break
			}
			return clusterVersion.Version, nil
		}
	}
	return "", ptr(missing(result, ReasonNoClusterReceipt,
		fmt.Sprintf("no dispatch receipt recorded for cluster %q for the pinned input", clusterName)))
}

// recordedPropagationStatus translates an explicit per-cluster propagation
// status into a Missing/Error result. isTerminal is false for ClusterPropagationOK,
// in which case member observation decides.
func recordedPropagationStatus(
	fedObject fedcorev1a1.GenericFederatedObject,
	clusterName string,
	result TargetResult,
) (TargetResult, bool) {
	for _, clusterStatus := range fedObject.GetStatus().Clusters {
		if clusterStatus.Cluster != clusterName {
			continue
		}
		switch clusterStatus.Status {
		case fedcorev1a1.ClusterPropagationOK:
			return result, false
		case fedcorev1a1.PendingCreate:
			return missing(result, ReasonPendingCreate,
				fmt.Sprintf("propagation to cluster %q is pending creation", clusterName)), true
		case fedcorev1a1.WaitingForRemoval, fedcorev1a1.WaitingForCascadingDeletion:
			return unobserved(result, ReasonRemovalPending,
				fmt.Sprintf("object in cluster %q is pending removal", clusterName)), true
		default:
			return errored(result, ReasonPropagationError,
				fmt.Sprintf("explicit propagation error in cluster %q: %s", clusterName, clusterStatus.Status)), true
		}
	}
	return result, false
}

// sourceGenerationMismatch verifies the propagated source-generation
// annotation when both the template generation and the annotation are present.
func sourceGenerationMismatch(template, memberObject *unstructured.Unstructured) bool {
	templateGeneration := template.GetGeneration()
	if templateGeneration == 0 {
		return false
	}
	annotations := memberObject.GetAnnotations()
	observed, ok := annotations[common.SourceGenerationAnnotation]
	if !ok {
		return false
	}
	return observed != strconv.FormatInt(templateGeneration, 10)
}

func stale(result TargetResult, reason, message string) TargetResult {
	result.State = TargetStale
	result.Reason = reason
	result.Message = message
	return result
}

func missing(result TargetResult, reason, message string) TargetResult {
	result.State = TargetMissing
	result.Reason = reason
	result.Message = message
	return result
}

func unobserved(result TargetResult, reason, message string) TargetResult {
	result.State = TargetUnobserved
	result.Reason = reason
	result.Message = message
	return result
}

func errored(result TargetResult, reason, message string) TargetResult {
	result.State = TargetError
	result.Reason = reason
	result.Message = message
	return result
}

func ptr(result TargetResult) *TargetResult {
	return &result
}
