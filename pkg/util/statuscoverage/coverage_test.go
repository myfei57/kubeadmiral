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

package statuscoverage

import (
	"errors"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"

	fedcorev1a1 "github.com/kubewharf/kubeadmiral/pkg/apis/core/v1alpha1"
	"github.com/kubewharf/kubeadmiral/pkg/controllers/common"
)

const (
	testFieldReplicas      = "status.replicas"
	testFieldReadyReplicas = "status.readyReplicas"
)

func newFedObject(
	generation, syncedGeneration int64,
	clusters []string,
	generationMap map[string]int64,
) *fedcorev1a1.FederatedObject {
	placements := make([]fedcorev1a1.ClusterReference, 0, len(clusters))
	propagation := make([]fedcorev1a1.PropagationStatus, 0, len(clusters))
	for _, cluster := range clusters {
		placements = append(placements, fedcorev1a1.ClusterReference{Cluster: cluster})
		propagation = append(propagation, fedcorev1a1.PropagationStatus{
			Cluster:                cluster,
			Status:                 fedcorev1a1.ClusterPropagationOK,
			LastObservedGeneration: generationMap[cluster],
		})
	}
	return &fedcorev1a1.FederatedObject{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "fed-deploy",
			Generation: generation,
			Annotations: map[string]string{
				common.SourceGenerationAnnotation: "5",
			},
		},
		Spec: fedcorev1a1.GenericFederatedObjectSpec{
			Placements: []fedcorev1a1.PlacementWithController{{
				Controller: "scheduler",
				Placement:  placements,
			}},
		},
		Status: fedcorev1a1.GenericFederatedObjectStatus{
			SyncedGeneration: syncedGeneration,
			Clusters:         propagation,
		},
	}
}

func newMemberObject(uid, resourceVersion string, generation int64, status map[string]interface{}) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata": map[string]interface{}{
			"name":            "deploy",
			"uid":             uid,
			"resourceVersion": resourceVersion,
			"generation":      int64(generation),
		},
		"status": status,
	}}
}

func readyObservation(cluster string, object *unstructured.Unstructured) MemberObservation {
	return MemberObservation{
		ClusterName: cluster,
		ClusterUID:  types.UID("cluster-uid-" + cluster),
		Ready:       true,
		Object:      object,
	}
}

func findClusterCoverage(t *testing.T, envelope *fedcorev1a1.CollectedStatusCoverage, cluster string) *fedcorev1a1.ClusterCoverage {
	t.Helper()
	for i := range envelope.Clusters {
		if envelope.Clusters[i].Cluster == cluster {
			return &envelope.Clusters[i]
		}
	}
	t.Fatalf("cluster %q unexpectedly missing from coverage: %+v", cluster, envelope.Clusters)
	return nil
}

func findFieldCoverage(t *testing.T, entry *fedcorev1a1.ClusterCoverage, path string) *fedcorev1a1.FieldCoverage {
	t.Helper()
	for i := range entry.Fields {
		if entry.Fields[i].Path == path {
			return &entry.Fields[i]
		}
	}
	t.Fatalf("field %q unexpectedly missing from cluster entry %q", path, entry.Cluster)
	return nil
}

func TestBuildCoverage_StrictAllCompleteWithRealZero(t *testing.T) {
	fedObject := newFedObject(3, 3, []string{"c1", "c2"}, map[string]int64{"c1": 3, "c2": 3})
	fields := []string{testFieldReplicas, testFieldReadyReplicas}

	c1Object := newMemberObject("uid-c1", "104729", 3, map[string]interface{}{
		"replicas":      int64(3),
		"readyReplicas": int64(2),
	})
	// c2 honestly reports zero replicas: this is a real observed zero, not a missing observation.
	c2Object := newMemberObject("uid-c2", "7", 3, map[string]interface{}{
		"replicas":      int64(0),
		"readyReplicas": int64(0),
	})

	envelope := Build(fedObject, []MemberObservation{
		readyObservation("c1", c1Object),
		readyObservation("c2", c2Object),
	}, BuildOptions{Strategy: fedcorev1a1.CoverageStrategyStrictAll, Fields: fields, SourceKind: "Deployment"})

	if !envelope.Complete {
		t.Fatalf("expected complete strict envelope, got %+v", envelope)
	}
	if envelope.Stale {
		t.Fatalf("did not expect stale envelope, reasons=%v", envelope.StaleReasons)
	}
	if envelope.TargetClusters != 2 || envelope.ObservedClusters != 2 {
		t.Fatalf("unexpected cluster counts: %+v", envelope)
	}
	if envelope.ProtocolVersion != fedcorev1a1.CoverageProtocolVersion {
		t.Fatalf("unexpected protocol version %q", envelope.ProtocolVersion)
	}
	if envelope.SourceGeneration != 5 || envelope.FederatedGeneration != 3 || envelope.SyncedGeneration != 3 {
		t.Fatalf("unexpected generations: %+v", envelope)
	}

	c1 := findClusterCoverage(t, envelope, "c1")
	if c1.State != fedcorev1a1.CoveragePresent || c1.MemberObjectUID != "uid-c1" {
		t.Fatalf("unexpected c1 entry: %+v", c1)
	}
	// resourceVersion is carried verbatim as an opaque string and never numerically compared.
	if c1.MemberObjectResourceVersion != "104729" {
		t.Fatalf("resourceVersion must be stored verbatim as an opaque identity, got %q", c1.MemberObjectResourceVersion)
	}

	c2Replicas := findFieldCoverage(t, findClusterCoverage(t, envelope, "c2"), testFieldReplicas)
	if c2Replicas.State != fedcorev1a1.CoveragePresent {
		t.Fatalf("real zero must be Present, got %s", c2Replicas.State)
	}
	if c2Replicas.Value == nil || string(c2Replicas.Value.Raw) != "0" {
		t.Fatalf("real zero must carry explicit value 0, got %+v", c2Replicas.Value)
	}
}

func TestBuildCoverage_UnreachableClusterStaysInEnvelope(t *testing.T) {
	fedObject := newFedObject(3, 3, []string{"c1", "c2"}, map[string]int64{"c1": 3, "c2": 3})
	c1Object := newMemberObject("uid-c1", "1", 3, map[string]interface{}{"replicas": int64(1)})

	envelope := Build(fedObject, []MemberObservation{
		readyObservation("c1", c1Object),
		{
			ClusterName: "c2",
			ClusterUID:  types.UID("cluster-uid-c2"),
			Ready:       true,
			ObjectError: errors.New("dial tcp: member cluster unreachable"),
		},
	}, BuildOptions{Strategy: fedcorev1a1.CoverageStrategyStrictAll, Fields: []string{testFieldReplicas}})

	if envelope.Complete {
		t.Fatalf("envelope with an unreachable cluster must not be complete")
	}
	if envelope.TargetClusters != 2 || envelope.ObservedClusters != 1 {
		t.Fatalf("unreachable cluster must not disappear from the target set: %+v", envelope)
	}
	c2 := findClusterCoverage(t, envelope, "c2")
	if c2.State != fedcorev1a1.CoverageError || c2.Reason != fedcorev1a1.CoverageReasonClusterUnreachable {
		t.Fatalf("unexpected c2 state: %+v", c2)
	}
	if !strings.Contains(c2.Error, "unreachable") {
		t.Fatalf("expected error message to be retained, got %q", c2.Error)
	}
	for _, field := range c2.Fields {
		if field.State != fedcorev1a1.CoverageUnknown || field.Value != nil {
			t.Fatalf("fields of an unreachable cluster must be Unknown without value, got %+v", field)
		}
	}
}

func TestBuildCoverage_NotReadyClusterStaysInEnvelope(t *testing.T) {
	fedObject := newFedObject(3, 3, []string{"c1", "c2"}, map[string]int64{"c1": 3, "c2": 3})
	c1Object := newMemberObject("uid-c1", "1", 3, map[string]interface{}{"replicas": int64(1)})

	envelope := Build(fedObject, []MemberObservation{
		readyObservation("c1", c1Object),
		{ClusterName: "c2", ClusterUID: types.UID("cluster-uid-c2"), Ready: false},
	}, BuildOptions{Strategy: fedcorev1a1.CoverageStrategyStrictAll, Fields: []string{testFieldReplicas}})

	c2 := findClusterCoverage(t, envelope, "c2")
	if c2.State != fedcorev1a1.CoverageUnknown || c2.Reason != fedcorev1a1.CoverageReasonClusterNotReady {
		t.Fatalf("not-ready cluster must stay as Unknown/ClusterNotReady, got %+v", c2)
	}
	if envelope.Complete || envelope.ObservedClusters != 1 {
		t.Fatalf("not-ready cluster must block completeness: %+v", envelope)
	}
}

func TestBuildCoverage_RebuiltObjectIsNotCurrentReceipt(t *testing.T) {
	fedObject := newFedObject(3, 3, []string{"c1", "c2"}, map[string]int64{"c1": 3, "c2": 3})
	c1Object := newMemberObject("uid-c1", "1", 3, map[string]interface{}{"replicas": int64(1)})
	// c2's same-named object was recreated: a fresh UID with a generation reset to 1 while the
	// propagated receipt expects generation 3. Its values must not count for the current generation.
	rebuiltObject := newMemberObject("uid-c2-rebuilt", "9", 1, map[string]interface{}{"replicas": int64(99)})

	envelope := Build(fedObject, []MemberObservation{
		readyObservation("c1", c1Object),
		readyObservation("c2", rebuiltObject),
	}, BuildOptions{Strategy: fedcorev1a1.CoverageStrategyStrictAll, Fields: []string{testFieldReplicas}})

	c2 := findClusterCoverage(t, envelope, "c2")
	if c2.State != fedcorev1a1.CoverageUnknown || c2.Reason != fedcorev1a1.CoverageReasonObjectRebuilt {
		t.Fatalf("rebuilt object must be Unknown/ObjectRebuilt, got %+v", c2)
	}
	// The identity of the misleading object is still recorded for diagnostics.
	if c2.MemberObjectUID != "uid-c2-rebuilt" || c2.MemberObjectResourceVersion != "9" {
		t.Fatalf("rebuilt object identity should be recorded, got %+v", c2)
	}
	for _, field := range c2.Fields {
		if field.State != fedcorev1a1.CoverageUnknown {
			t.Fatalf("rebuilt object fields must not be accepted, got %+v", field)
		}
	}
	if envelope.Complete || !envelope.Stale {
		t.Fatalf("rebuilt object must block completeness and mark the envelope stale: %+v", envelope)
	}
}

func TestBuildCoverage_PropagationNotSettledIsStaleAndIncomplete(t *testing.T) {
	// Federated generation 4 has not synced yet (syncedGeneration stays 3).
	fedObject := newFedObject(4, 3, []string{"c1"}, map[string]int64{"c1": 3})
	c1Object := newMemberObject("uid-c1", "1", 3, map[string]interface{}{"replicas": int64(1)})

	envelope := Build(fedObject, []MemberObservation{
		readyObservation("c1", c1Object),
	}, BuildOptions{Strategy: fedcorev1a1.CoverageStrategyStrictAll, Fields: []string{testFieldReplicas}})

	if envelope.Complete {
		t.Fatalf("an envelope built before propagation settles cannot be complete")
	}
	if !envelope.Stale || !containsReason(envelope.StaleReasons, fedcorev1a1.CoverageStaleGeneration) {
		t.Fatalf("expected stale generation envelope, got %+v", envelope)
	}
}

func TestBuildCoverage_PartialStrategyNeverComplete(t *testing.T) {
	fedObject := newFedObject(3, 3, []string{"c1"}, map[string]int64{"c1": 3})
	c1Object := newMemberObject("uid-c1", "1", 3, map[string]interface{}{"replicas": int64(1)})

	envelope := Build(fedObject, []MemberObservation{
		readyObservation("c1", c1Object),
	}, BuildOptions{Strategy: fedcorev1a1.CoverageStrategyPartial, Fields: []string{testFieldReplicas}})

	if envelope.Complete {
		t.Fatalf("partial envelopes must never be marked complete even when everything is observed")
	}
	if envelope.Strategy != fedcorev1a1.CoverageStrategyPartial || envelope.ObservedClusters != 1 {
		t.Fatalf("unexpected partial envelope: %+v", envelope)
	}
}

func TestBuildCoverage_AbsentFieldAndUnknownAreDistinct(t *testing.T) {
	fedObject := newFedObject(3, 3, []string{"c1"}, map[string]int64{"c1": 3})
	// Only replicas exists; readyReplicas is absent from the object.
	c1Object := newMemberObject("uid-c1", "1", 3, map[string]interface{}{"replicas": int64(2)})

	envelope := Build(fedObject, []MemberObservation{
		readyObservation("c1", c1Object),
	}, BuildOptions{
		Strategy:   fedcorev1a1.CoverageStrategyStrictAll,
		Fields:     []string{testFieldReplicas, testFieldReadyReplicas},
		SourceKind: "Deployment",
	})

	c1 := findClusterCoverage(t, envelope, "c1")
	replicas := findFieldCoverage(t, c1, testFieldReplicas)
	if replicas.State != fedcorev1a1.CoveragePresent || string(replicas.Value.Raw) != "2" {
		t.Fatalf("expected Present value 2, got %+v", replicas)
	}
	readyReplicas := findFieldCoverage(t, c1, testFieldReadyReplicas)
	if readyReplicas.State != fedcorev1a1.CoverageAbsent || readyReplicas.Value != nil {
		t.Fatalf("missing field must be Absent without value, got %+v", readyReplicas)
	}
	if envelope.Complete {
		t.Fatalf("an absent field must block strict completeness")
	}
}

func TestBuildCoverage_PlacementRemovalStalesPreviousEnvelope(t *testing.T) {
	oldFedObject := newFedObject(3, 3, []string{"c1", "c2"}, map[string]int64{"c1": 3, "c2": 3})
	oldEnvelope := Build(oldFedObject, []MemberObservation{
		readyObservation("c1", newMemberObject("uid-c1", "1", 3, map[string]interface{}{"replicas": int64(1)})),
		readyObservation("c2", newMemberObject("uid-c2", "1", 3, map[string]interface{}{"replicas": int64(1)})),
	}, BuildOptions{Strategy: fedcorev1a1.CoverageStrategyStrictAll, Fields: []string{testFieldReplicas}})

	// Placement shrinks: c2 is removed from the explicit target set.
	newFedObject := newFedObject(3, 3, []string{"c1"}, map[string]int64{"c1": 3})
	newEnvelope := Build(newFedObject, []MemberObservation{
		readyObservation("c1", newMemberObject("uid-c1", "1", 3, map[string]interface{}{"replicas": int64(1)})),
	}, BuildOptions{
		Strategy: fedcorev1a1.CoverageStrategyStrictAll,
		Fields:   []string{testFieldReplicas},
		Previous: oldEnvelope,
	})

	if newEnvelope.TargetClusters != 1 || len(newEnvelope.Clusters) != 1 {
		t.Fatalf("removed cluster must disappear from the new envelope: %+v", newEnvelope)
	}
	if !newEnvelope.Stale || !containsReason(newEnvelope.StaleReasons, fedcorev1a1.CoverageStalePlacement) {
		t.Fatalf("placement removal must stale the previous envelope: %+v", newEnvelope)
	}
	if newEnvelope.PlacementHash == oldEnvelope.PlacementHash {
		t.Fatalf("placement hash must change when the target set changes")
	}
}

func TestBuildCoverage_ConfigChangeStalesPreviousEnvelope(t *testing.T) {
	fedObject := newFedObject(3, 3, []string{"c1"}, map[string]int64{"c1": 3})
	object := newMemberObject("uid-c1", "1", 3, map[string]interface{}{
		"replicas":      int64(1),
		"readyReplicas": int64(1),
	})
	previous := Build(fedObject, []MemberObservation{readyObservation("c1", object)},
		BuildOptions{Strategy: fedcorev1a1.CoverageStrategyStrictAll, Fields: []string{testFieldReplicas}})

	latest := Build(fedObject, []MemberObservation{readyObservation("c1", object)}, BuildOptions{
		Strategy: fedcorev1a1.CoverageStrategyStrictAll,
		Fields:   []string{testFieldReplicas, testFieldReadyReplicas},
		Previous: previous,
	})

	if !latest.Stale || !containsReason(latest.StaleReasons, fedcorev1a1.CoverageStaleConfig) {
		t.Fatalf("config change must stale the previous envelope: %+v", latest)
	}
}

func TestBuildCoverage_SecretFieldsAreNotCollected(t *testing.T) {
	fedObject := newFedObject(1, 1, []string{"c1"}, map[string]int64{"c1": 1})
	secretObject := &unstructured.Unstructured{Object: map[string]interface{}{
		"kind": "Secret",
		"metadata": map[string]interface{}{
			"name":       "secret",
			"uid":        "uid-secret",
			"generation": int64(1),
		},
		"data":       map[string]interface{}{"token": "c2VjcmV0"},
		"stringData": map[string]interface{}{"other": "value"},
	}}

	envelope := Build(fedObject, []MemberObservation{readyObservation("c1", secretObject)}, BuildOptions{
		Strategy:   fedcorev1a1.CoverageStrategyStrictAll,
		Fields:     []string{"metadata.name", "data.token", "stringData.other"},
		SourceKind: "Secret",
	})

	c1 := findClusterCoverage(t, envelope, "c1")
	nameField := findFieldCoverage(t, c1, "metadata.name")
	if nameField.State != fedcorev1a1.CoveragePresent {
		t.Fatalf("non-secret metadata must remain collectable, got %+v", nameField)
	}
	for _, path := range []string{"data.token", "stringData.other"} {
		field := findFieldCoverage(t, c1, path)
		if field.State != fedcorev1a1.CoverageError || field.Value != nil ||
			field.Error != fedcorev1a1.CoverageReasonSecretFieldRefused {
			t.Fatalf("secret field %q must be refused without leaking its value, got %+v", path, field)
		}
	}
}

func TestBuildCoverage_EnforcesClusterFieldAndErrorLimits(t *testing.T) {
	targets := []string{"c1", "c2", "c3", "c4"}
	fedObject := newFedObject(1, 1, targets, map[string]int64{"c1": 1, "c2": 1, "c3": 1, "c4": 1})
	observations := []MemberObservation{
		readyObservation("c1", newMemberObject("uid-c1", "1", 1, map[string]interface{}{"replicas": int64(1)})),
		readyObservation("c2", newMemberObject("uid-c2", "1", 1, map[string]interface{}{"replicas": int64(1)})),
		// c3 is within the cluster cap, so its oversized error message must be byte-truncated.
		{ClusterName: "c3", Ready: true, ObjectError: errors.New(strings.Repeat("x", 3000))},
		// c4 is pushed out by the cluster cap but must remain visible in TargetClusters.
		readyObservation("c4", newMemberObject("uid-c4", "1", 1, map[string]interface{}{"replicas": int64(1)})),
	}

	envelope := Build(fedObject, observations, BuildOptions{
		Strategy:      fedcorev1a1.CoverageStrategyStrictAll,
		Fields:        []string{testFieldReplicas, testFieldReadyReplicas},
		MaxClusters:   3,
		MaxFields:     1,
		MaxErrorBytes: 128,
	})

	if !envelope.Truncated {
		t.Fatalf("limit breaches must set Truncated")
	}
	if envelope.TargetClusters != 4 || len(envelope.Clusters) != 3 {
		t.Fatalf("target count must stay visible while entries are capped: %+v", envelope)
	}
	c3 := findClusterCoverage(t, envelope, "c3")
	if len(c3.Error) != 128 {
		t.Fatalf("error messages must be truncated to exactly 128 bytes, got %d", len(c3.Error))
	}
	for _, entry := range envelope.Clusters {
		if len(entry.Fields) != 1 {
			t.Fatalf("field limit must cap per-cluster fields, got %d", len(entry.Fields))
		}
	}
	if findClusterCoverageOrNil(envelope, "c4") != nil {
		t.Fatalf("c4 should have been dropped by the cluster cap")
	}
}

func findClusterCoverageOrNil(envelope *fedcorev1a1.CollectedStatusCoverage, cluster string) *fedcorev1a1.ClusterCoverage {
	for i := range envelope.Clusters {
		if envelope.Clusters[i].Cluster == cluster {
			return &envelope.Clusters[i]
		}
	}
	return nil
}

func TestBuildCoverage_ObjectAbsentIsRecorded(t *testing.T) {
	fedObject := newFedObject(3, 3, []string{"c1"}, map[string]int64{"c1": 3})
	envelope := Build(fedObject, []MemberObservation{{
		ClusterName: "c1",
		ClusterUID:  types.UID("cluster-uid-c1"),
		Ready:       true,
		ObjectAbsent: true,
	}}, BuildOptions{Strategy: fedcorev1a1.CoverageStrategyStrictAll, Fields: []string{testFieldReplicas}})

	c1 := findClusterCoverage(t, envelope, "c1")
	if c1.State != fedcorev1a1.CoverageAbsent || c1.Reason != fedcorev1a1.CoverageReasonObjectAbsent {
		t.Fatalf("absent object must be recorded as Absent/ObjectAbsent, got %+v", c1)
	}
	if envelope.Complete || envelope.ObservedClusters != 0 {
		t.Fatalf("an absent object must not count as observed: %+v", envelope)
	}
}

func TestTargetClusterNames_UnionSortedDeduped(t *testing.T) {
	fedObject := &fedcorev1a1.FederatedObject{
		Spec: fedcorev1a1.GenericFederatedObjectSpec{
			Placements: []fedcorev1a1.PlacementWithController{
				{Controller: "a", Placement: []fedcorev1a1.ClusterReference{{Cluster: "c2"}, {Cluster: "c1"}}},
				{Controller: "b", Placement: []fedcorev1a1.ClusterReference{{Cluster: "c1"}, {Cluster: "c3"}}},
			},
		},
	}
	names := TargetClusterNames(fedObject)
	expected := []string{"c1", "c2", "c3"}
	if len(names) != len(expected) {
		t.Fatalf("expected %v, got %v", expected, names)
	}
	for i := range expected {
		if names[i] != expected[i] {
			t.Fatalf("expected sorted union %v, got %v", expected, names)
		}
	}
}

func TestHashes_StableAndOrderIndependent(t *testing.T) {
	h1 := ConfigHash(fedcorev1a1.CoverageStrategyStrictAll, []string{"b", "a"})
	h2 := ConfigHash(fedcorev1a1.CoverageStrategyStrictAll, []string{"a", "b"})
	if h1 != h2 || h1 == "" {
		t.Fatalf("config hash must be stable and order-independent, got %q vs %q", h1, h2)
	}
	h3 := ConfigHash(fedcorev1a1.CoverageStrategyPartial, []string{"a", "b"})
	if h3 == h1 {
		t.Fatalf("different strategies must produce different hashes")
	}
	p1 := PlacementHash([]string{"c2", "c1"})
	p2 := PlacementHash([]string{"c1", "c2"})
	if p1 != p2 || p1 == "" {
		t.Fatalf("placement hash must be stable and order-independent, got %q vs %q", p1, p2)
	}
}

func containsReason(reasons []string, reason string) bool {
	for _, candidate := range reasons {
		if candidate == reason {
			return true
		}
	}
	return false
}
