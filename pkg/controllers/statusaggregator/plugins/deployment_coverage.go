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

package plugins

import (
	"context"
	"encoding/json"
	"reflect"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"

	fedcorev1a1 "github.com/kubewharf/kubeadmiral/pkg/apis/core/v1alpha1"
	"github.com/kubewharf/kubeadmiral/pkg/controllers/common"
	"github.com/kubewharf/kubeadmiral/pkg/util/annotation"
	"github.com/kubewharf/kubeadmiral/pkg/util/statuscoverage"
)

// deploymentReplicaFields maps collected deployment status field paths to their strongly typed
// counterparts. Only integral replica fields are aggregated precisely.
var deploymentReplicaFields = []struct {
	path string
	set  func(*appsv1.DeploymentStatus, int32)
}{
	{path: "status.replicas", set: func(s *appsv1.DeploymentStatus, v int32) { s.Replicas = v }},
	{path: "status.updatedReplicas", set: func(s *appsv1.DeploymentStatus, v int32) { s.UpdatedReplicas = v }},
	{path: "status.readyReplicas", set: func(s *appsv1.DeploymentStatus, v int32) { s.ReadyReplicas = v }},
	{path: "status.availableReplicas", set: func(s *appsv1.DeploymentStatus, v int32) { s.AvailableReplicas = v }},
	{path: "status.unavailableReplicas", set: func(s *appsv1.DeploymentStatus, v int32) { s.UnavailableReplicas = v }},
}

// SupportsCoverage declares that the deployment plugin understands the coverage envelope
// protocol.
func (receiver *DeploymentPlugin) SupportsCoverage() bool {
	return true
}

// AggregateCoverage consumes a CollectedStatusCoverage envelope instead of raw member objects.
//
// Precise status fields are written only when the envelope is a complete, non-stale StrictAll
// envelope AND the retained IsResourcePropagated gate passes. Otherwise the standard status fields
// are left untouched (missing replicas are never treated as zero) and the partial result is
// published as an explicit range summary in the status-coverage-aggregate annotation, which callers
// cannot mistake for complete health.
func (receiver *DeploymentPlugin) AggregateCoverage(
	ctx context.Context,
	sourceObject *unstructured.Unstructured,
	fedObject fedcorev1a1.GenericFederatedObject,
	coverage *fedcorev1a1.CollectedStatusCoverage,
	resourcePropagated bool,
) (*unstructured.Unstructured, bool, error) {
	logger := klog.FromContext(ctx).WithValues("status-aggregator-plugin", "deployments-coverage")

	aggregated, err := statuscoverage.Aggregate(coverage)
	if err != nil {
		return nil, false, err
	}

	sourceDeployment := &appsv1.Deployment{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(sourceObject.Object, sourceDeployment); err != nil {
		return nil, false, err
	}

	needUpdate := false

	// The IsResourcePropagated gate is intentionally retained: even a complete envelope is not
	// allowed to publish a precise aggregate while the source template has not fully propagated.
	if resourcePropagated && aggregated.IsComplete() &&
		coverage.Strategy == fedcorev1a1.CoverageStrategyStrictAll {
		aggregatedStatus := &appsv1.DeploymentStatus{}
		for _, field := range deploymentReplicaFields {
			fieldAggregate := aggregated.FindField(field.path)
			if fieldAggregate == nil {
				continue
			}
			field.set(aggregatedStatus, int32(fieldAggregate.ObservedValue))
		}
		// Every member object acknowledged the current propagation generation, so the source
		// generation is now fully observed.
		aggregatedStatus.ObservedGeneration = sourceDeployment.Generation

		newStatus, err := runtime.DefaultUnstructuredConverter.ToUnstructured(aggregatedStatus)
		if err != nil {
			return nil, false, err
		}
		oldStatus, _, err := unstructured.NestedMap(sourceObject.Object, common.StatusField)
		if err != nil {
			return nil, false, err
		}
		if !reflect.DeepEqual(newStatus, oldStatus) {
			if err := unstructured.SetNestedMap(sourceObject.Object, newStatus, common.StatusField); err != nil {
				return nil, false, err
			}
			needUpdate = true
		}
	} else {
		logger.V(3).Info("Coverage is incomplete or propagation gate is open; precise status is not published",
			"complete", coverage.Complete, "stale", coverage.Stale, "resourcePropagated", resourcePropagated)
	}

	// The annotation always carries the explicit completeness marker and per-field ranges, so
	// callers can distinguish a healthy whole-fleet aggregate from a partial observation.
	annotationBytes, err := json.Marshal(aggregated)
	if err != nil {
		return nil, false, err
	}
	annotationValue := string(annotationBytes)
	hasAnnotation, err := annotation.HasAnnotationKeyValue(
		sourceObject,
		common.StatusCoverageAggregateAnnotation,
		annotationValue,
	)
	if err != nil {
		return nil, false, err
	}
	if !hasAnnotation {
		if _, err := annotation.AddAnnotation(sourceObject, common.StatusCoverageAggregateAnnotation, annotationValue); err != nil {
			return nil, false, err
		}
		needUpdate = true
	}

	return sourceObject, needUpdate, nil
}
