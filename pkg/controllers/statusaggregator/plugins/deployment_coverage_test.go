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
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"

	fedcorev1a1 "github.com/kubewharf/kubeadmiral/pkg/apis/core/v1alpha1"
	"github.com/kubewharf/kubeadmiral/pkg/controllers/common"
	"github.com/kubewharf/kubeadmiral/pkg/util/statuscoverage"
)

var deployCoverageFields = []string{
	"status.replicas",
	"status.updatedReplicas",
	"status.readyReplicas",
	"status.availableReplicas",
	"status.unavailableReplicas",
}

func coverageFedObject(clusters []string) *fedcorev1a1.FederatedObject {
	placement := make([]fedcorev1a1.ClusterReference, 0, len(clusters))
	propagation := make([]fedcorev1a1.PropagationStatus, 0, len(clusters))
	for _, cluster := range clusters {
		placement = append(placement, fedcorev1a1.ClusterReference{Cluster: cluster})
		propagation = append(propagation, fedcorev1a1.PropagationStatus{
			Cluster:                cluster,
			Status:                 fedcorev1a1.ClusterPropagationOK,
			LastObservedGeneration: 2,
		})
	}
	return &fedcorev1a1.FederatedObject{
		ObjectMeta: metav1.ObjectMeta{Name: "fed-deploy", Generation: 2},
		Spec: fedcorev1a1.GenericFederatedObjectSpec{
			Placements: []fedcorev1a1.PlacementWithController{{Controller: "scheduler", Placement: placement}},
		},
		Status: fedcorev1a1.GenericFederatedObjectStatus{
			SyncedGeneration: 2,
			Clusters:         propagation,
		},
	}
}

func memberDeployment(uid string, status appsv1.DeploymentStatus) *unstructured.Unstructured {
	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "deploy",
			UID:             types.UID(uid),
			ResourceVersion: "rv-" + uid,
			Generation:      2,
		},
		Status: status,
	}
	object, err := runtime.DefaultUnstructuredConverter.ToUnstructured(deploy)
	if err != nil {
		panic(err)
	}
	return &unstructured.Unstructured{Object: object}
}

func sourceDeployment() *unstructured.Unstructured {
	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "deploy", Namespace: "default", Generation: 7},
	}
	object, err := runtime.DefaultUnstructuredConverter.ToUnstructured(deploy)
	if err != nil {
		panic(err)
	}
	return &unstructured.Unstructured{Object: object}
}

func coverageEnvelope(
	t *testing.T,
	fedObject *fedcorev1a1.FederatedObject,
	observations []statuscoverage.MemberObservation,
	strategy fedcorev1a1.CoverageStrategy,
) *fedcorev1a1.CollectedStatusCoverage {
	t.Helper()
	envelope := statuscoverage.Build(fedObject, observations, statuscoverage.BuildOptions{
		Strategy:   strategy,
		Fields:     deployCoverageFields,
		SourceKind: common.DeploymentKind,
	})
	return envelope
}

func TestGetCoveragePlugin_Declaration(t *testing.T) {
	plugin := GetCoveragePlugin(appsv1.SchemeGroupVersion.WithKind(common.DeploymentKind))
	if plugin == nil || !plugin.SupportsCoverage() {
		t.Fatalf("deployment plugin must declare coverage support")
	}
	// Plugins that have not opted in must not be handed envelopes.
	if GetCoveragePlugin(appsv1.SchemeGroupVersion.WithKind(common.StatefulSetKind)) != nil {
		t.Fatalf("single-cluster plugin must not declare coverage support")
	}
}

// TestDeploymentCoverage_StrictParityWithLegacyAggregator cross-checks the coverage-based strict
// aggregation against the original member-object-based DeploymentPlugin on one fixed complete
// snapshot.
func TestDeploymentCoverage_StrictParityWithLegacyAggregator(t *testing.T) {
	ctx := klog.NewContext(context.Background(), klog.Background())
	clusterNames := []string{"c1", "c2", "c3"}
	fedObject := coverageFedObject(clusterNames)

	memberStatuses := map[string]appsv1.DeploymentStatus{
		"c1": {Replicas: 3, UpdatedReplicas: 3, ReadyReplicas: 2, AvailableReplicas: 2, UnavailableReplicas: 1, ObservedGeneration: 2},
		"c3": {Replicas: 5, UpdatedReplicas: 4, ReadyReplicas: 5, AvailableReplicas: 4, UnavailableReplicas: 1, ObservedGeneration: 2},
	}
	// c2 explicitly carries real zeroes in its status JSON (rather than omitting the fields), so
	// both aggregators observe honest zero values instead of missing fields.
	zeroObject := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata": map[string]interface{}{
			"name": "deploy", "uid": "uid-c2", "resourceVersion": "rv-uid-c2", "generation": int64(2),
		},
		"status": map[string]interface{}{
			"replicas":            int64(0),
			"updatedReplicas":     int64(0),
			"readyReplicas":       int64(0),
			"availableReplicas":   int64(0),
			"unavailableReplicas": int64(0),
			"observedGeneration":  int64(2),
		},
	}}

	clusterObjs := map[string]interface{}{}
	observations := make([]statuscoverage.MemberObservation, 0, len(clusterNames))
	for _, name := range clusterNames {
		var object *unstructured.Unstructured
		if name == "c2" {
			object = zeroObject
		} else {
			object = memberDeployment("uid-"+name, memberStatuses[name])
		}
		clusterObjs[name] = object
		observations = append(observations, statuscoverage.MemberObservation{
			ClusterName: name,
			ClusterUID:  types.UID("cluster-uid-" + name),
			Ready:       true,
			Object:      object,
		})
	}

	// Reference result produced by the original aggregator on the raw member objects.
	legacyPlugin := NewDeploymentPlugin()
	legacyResult, legacyNeedUpdate, err := legacyPlugin.AggregateStatuses(
		ctx, sourceDeployment(), fedObject, clusterObjs, true,
	)
	if err != nil {
		t.Fatalf("legacy aggregation failed: %v", err)
	}
	if !legacyNeedUpdate {
		t.Fatalf("legacy aggregation should update the empty source status")
	}
	legacyStatus, _, err := unstructured.NestedMap(legacyResult.Object, common.StatusField)
	if err != nil {
		t.Fatalf("failed to read legacy status: %v", err)
	}

	// Result produced by the coverage aggregator from the envelope describing the same snapshot.
	envelope := coverageEnvelope(t, fedObject, observations, fedcorev1a1.CoverageStrategyStrictAll)
	if !envelope.Complete {
		t.Fatalf("test setup requires a complete envelope, got %+v", envelope)
	}
	coverageResult, coverageNeedUpdate, err := legacyPlugin.AggregateCoverage(
		ctx, sourceDeployment(), fedObject, envelope, true,
	)
	if err != nil {
		t.Fatalf("coverage aggregation failed: %v", err)
	}
	if !coverageNeedUpdate {
		t.Fatalf("coverage aggregation should update the empty source status")
	}
	coverageStatus, _, err := unstructured.NestedMap(coverageResult.Object, common.StatusField)
	if err != nil {
		t.Fatalf("failed to read coverage status: %v", err)
	}

	for _, field := range []string{
		"replicas", "updatedReplicas", "readyReplicas", "availableReplicas", "unavailableReplicas",
		"observedGeneration",
	} {
		if !equalJSON(legacyStatus[field], coverageStatus[field]) {
			t.Fatalf("field %s mismatch: legacy=%v coverage=%v", field, legacyStatus[field], coverageStatus[field])
		}
	}

	// The annotation must explicitly advertise completeness for callers.
	annotationRaw := coverageResult.GetAnnotations()[common.StatusCoverageAggregateAnnotation]
	if annotationRaw == "" {
		t.Fatalf("complete aggregation must publish the coverage annotation")
	}
	summary := statuscoverage.AggregatedCoverage{}
	if err := json.Unmarshal([]byte(annotationRaw), &summary); err != nil {
		t.Fatalf("coverage annotation is not valid JSON: %v", err)
	}
	if !summary.Complete || summary.Stale {
		t.Fatalf("annotation must describe a complete, non-stale snapshot, got %+v", summary)
	}
}

// TestDeploymentCoverage_PartialNeverPublishesPreciseStatus verifies that a partial envelope only
// produces an explicit range annotation and never overwrites standard status fields with fabricated
// zeroes, and that callers cannot mistake it for complete health.
func TestDeploymentCoverage_PartialNeverPublishesPreciseStatus(t *testing.T) {
	ctx := klog.NewContext(context.Background(), klog.Background())
	fedObject := coverageFedObject([]string{"c1", "c2"})

	c1 := memberDeployment("uid-c1", appsv1.DeploymentStatus{
		Replicas: 4, UpdatedReplicas: 4, ReadyReplicas: 4, AvailableReplicas: 4, ObservedGeneration: 2,
	})
	envelope := coverageEnvelope(t, fedObject, []statuscoverage.MemberObservation{
		{ClusterName: "c1", ClusterUID: "cluster-uid-c1", Ready: true, Object: c1},
		{ClusterName: "c2", ClusterUID: "cluster-uid-c2", Ready: true, ObjectError: assertCoverageError("unreachable")},
	}, fedcorev1a1.CoverageStrategyPartial)

	source := sourceDeployment()
	// Pre-existing status that must survive untouched.
	if err := unstructured.SetNestedField(source.Object, int64(99), "status", "replicas"); err != nil {
		t.Fatalf("failed to seed source status: %v", err)
	}

	result, needUpdate, err := NewDeploymentPlugin().AggregateCoverage(ctx, source, fedObject, envelope, true)
	if err != nil {
		t.Fatalf("partial aggregation failed: %v", err)
	}

	replicas, found, err := unstructured.NestedInt64(result.Object, "status", "replicas")
	if err != nil || !found || replicas != 99 {
		t.Fatalf("partial aggregation must not touch precise status fields, got replicas=%d found=%v err=%v",
			replicas, found, err)
	}
	// The only update is the annotation.
	if !needUpdate {
		t.Fatalf("the range annotation should still be published")
	}

	summaryRaw := result.GetAnnotations()[common.StatusCoverageAggregateAnnotation]
	summary := statuscoverage.AggregatedCoverage{}
	if err := json.Unmarshal([]byte(summaryRaw), &summary); err != nil {
		t.Fatalf("invalid coverage annotation: %v", err)
	}
	if summary.Complete || summary.IsComplete() {
		t.Fatalf("partial summary must not be identifiable as complete health")
	}
	field := summary.FindField("status.replicas")
	if field == nil || field.UpperBound != nil || field.LowerBound != 4 || field.MissingClusters != 1 {
		t.Fatalf("partial replicas must be an unbounded range over 4 observed, got %+v", field)
	}
}

// TestDeploymentCoverage_ClosedGateBlocksPreciseAggregate verifies the retained
// IsResourcePropagated gate still blocks precise aggregation even with a complete envelope.
func TestDeploymentCoverage_ClosedGateBlocksPreciseAggregate(t *testing.T) {
	ctx := klog.NewContext(context.Background(), klog.Background())
	fedObject := coverageFedObject([]string{"c1"})
	c1 := memberDeployment("uid-c1", appsv1.DeploymentStatus{Replicas: 1, ObservedGeneration: 2})
	envelope := coverageEnvelope(t, fedObject, []statuscoverage.MemberObservation{
		{ClusterName: "c1", ClusterUID: "cluster-uid-c1", Ready: true, Object: c1},
	}, fedcorev1a1.CoverageStrategyStrictAll)

	source := sourceDeployment()
	if err := unstructured.SetNestedField(source.Object, int64(42), "status", "replicas"); err != nil {
		t.Fatalf("failed to seed source status: %v", err)
	}

	result, _, err := NewDeploymentPlugin().AggregateCoverage(ctx, source, fedObject, envelope, false)
	if err != nil {
		t.Fatalf("aggregation failed: %v", err)
	}
	replicas, found, err := unstructured.NestedInt64(result.Object, "status", "replicas")
	if err != nil || !found || replicas != 42 {
		t.Fatalf("closed propagation gate must block precise status writes, got replicas=%d found=%v", replicas, found)
	}
}

func equalJSON(a, b interface{}) bool {
	aa, _ := json.Marshal(a)
	bb, _ := json.Marshal(b)
	return string(aa) == string(bb)
}

type coverageErrString string

func (e coverageErrString) Error() string { return string(e) }

func assertCoverageError(message string) error { return coverageErrString(message) }
