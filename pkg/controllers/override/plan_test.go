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
	"errors"
	"reflect"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	jsonutil "k8s.io/apimachinery/pkg/util/json"

	fedcorev1a1 "github.com/kubewharf/kubeadmiral/pkg/apis/core/v1alpha1"
	ctrutil "github.com/kubewharf/kubeadmiral/pkg/controllers/util"
)

// mustFedObject wraps a template map into a FederatedObject with a fixed identity for binding tests.
func mustFedObject(t *testing.T, template map[string]interface{}) *fedcorev1a1.FederatedObject {
	t.Helper()
	raw, err := (&unstructured.Unstructured{Object: template}).MarshalJSON()
	if err != nil {
		t.Fatalf("failed to marshal template: %v", err)
	}
	return &fedcorev1a1.FederatedObject{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       "default",
			Name:            "fed-object",
			UID:             types.UID("uid-fed-object"),
			ResourceVersion: "100",
		},
		Spec: fedcorev1a1.GenericFederatedObjectSpec{
			Template: apiextensionsv1.JSON{Raw: raw},
		},
	}
}

func widgetTemplate() map[string]interface{} {
	return map[string]interface{}{
		"apiVersion": "example.com/v1",
		"kind":       "Widget",
		"metadata": map[string]interface{}{
			"name":   "widget",
			"labels": map[string]interface{}{},
		},
		"a": map[string]interface{}{"b": int64(1)},
		"items": []interface{}{
			map[string]interface{}{"id": "a"},
			map[string]interface{}{"id": "b"},
			map[string]interface{}{"id": "c"},
		},
	}
}

func podWithEnvsTemplate() map[string]interface{} {
	return map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "Pod",
		"metadata": map[string]interface{}{
			"name": "pod-test",
		},
		"spec": map[string]interface{}{
			"containers": []interface{}{
				map[string]interface{}{
					"name":  "server",
					"image": "docker.io/ealen/echo-server:latest",
					"env": []interface{}{
						map[string]interface{}{"name": "A", "value": "1"},
						map[string]interface{}{"name": "A", "value": "2"},
						map[string]interface{}{"name": "B", "value": "3"},
					},
				},
			},
		},
	}
}

func twoClusters() []*fedcorev1a1.FederatedCluster {
	return []*fedcorev1a1.FederatedCluster{
		{ObjectMeta: metav1.ObjectMeta{Name: "cluster1"}},
		{ObjectMeta: metav1.ObjectMeta{Name: "cluster2"}},
	}
}

func allClusterRule(overriders *fedcorev1a1.Overriders) fedcorev1a1.OverrideRule {
	return fedcorev1a1.OverrideRule{
		TargetClusters: &fedcorev1a1.TargetClusters{ClusterSelector: map[string]string{}},
		Overriders:     overriders,
	}
}

func namedClusterRule(cluster string, overriders *fedcorev1a1.Overriders) fedcorev1a1.OverrideRule {
	return fedcorev1a1.OverrideRule{
		TargetClusters: &fedcorev1a1.TargetClusters{Clusters: []string{cluster}},
		Overriders:     overriders,
	}
}

// referencePipeline runs the original parseOverrides/mergeOverrides pipeline and returns the raw
// patches per cluster together with the candidate bytes obtained by applying them (finalizer patches
// filtered exactly like the controller does).
func referencePipeline(
	t *testing.T,
	fedObject fedcorev1a1.GenericFederatedObject,
	policies []fedcorev1a1.GenericOverridePolicy,
	clusters []*fedcorev1a1.FederatedCluster,
) (map[string]fedcorev1a1.OverridePatches, map[string][]byte) {
	t.Helper()
	helpers := map[string]*helpData{}
	var merged overridesMap
	for _, policy := range policies {
		overrides, err := parseOverrides(policy, clusters, fedObject, helpers)
		if err != nil {
			t.Fatalf("reference parseOverrides failed: %v", err)
		}
		merged = mergeOverrides(merged, overrides)
	}

	candidates := make(map[string][]byte)
	rawPatches := make(map[string]fedcorev1a1.OverridePatches)
	for clusterName, patches := range merged {
		template, err := fedObject.GetSpec().GetTemplateAsUnstructured()
		if err != nil {
			t.Fatalf("failed to decode template: %v", err)
		}
		if err = ctrutil.ApplyJSONPatch(template, ignoreFinalizerPatches(patches)); err != nil {
			t.Fatalf("reference ApplyJSONPatch failed: %v", err)
		}
		raw, err := template.MarshalJSON()
		if err != nil {
			t.Fatalf("reference marshal failed: %v", err)
		}
		candidates[clusterName] = raw
		rawPatches[clusterName] = patches
	}
	return rawPatches, candidates
}

// assertMatchesReferencePipeline verifies raw patch bytes/order and candidate bytes are identical to
// the original pipeline, for every cluster in the plan.
func assertMatchesReferencePipeline(
	t *testing.T,
	plan *OverridePatchPlan,
	fedObject fedcorev1a1.GenericFederatedObject,
	policies []fedcorev1a1.GenericOverridePolicy,
	clusters []*fedcorev1a1.FederatedCluster,
) {
	t.Helper()
	rawPatches, candidates := referencePipeline(t, fedObject, policies, clusters)
	assert.Len(t, plan.ClusterPlans, len(clusters))
	for _, clusterPlan := range plan.ClusterPlans {
		expectedRaw, ok := rawPatches[clusterPlan.Cluster]
		if !ok {
			t.Fatalf("reference pipeline has no patches for cluster %q", clusterPlan.Cluster)
		}
		assert.Len(t, clusterPlan.Patches, len(expectedRaw), "cluster %q patch count", clusterPlan.Cluster)
		for i, expected := range expectedRaw {
			assert.True(t, equalRawPatch(expected, clusterPlan.Patches[i].Patch),
				"cluster %q patch %d differs: expected %+v, got %+v",
				clusterPlan.Cluster, i, expected, clusterPlan.Patches[i].Patch)
		}
		rawCandidate, ok := plan.RawCandidateBytes(clusterPlan.Cluster)
		assert.True(t, ok)
		assert.Equal(t, candidates[clusterPlan.Cluster], rawCandidate,
			"cluster %q candidate bytes differ from reference pipeline", clusterPlan.Cluster)
	}
}

func findClusterPlan(t *testing.T, plan *OverridePatchPlan, cluster string) ClusterPatchPlan {
	t.Helper()
	for _, clusterPlan := range plan.ClusterPlans {
		if clusterPlan.Cluster == cluster {
			return clusterPlan
		}
	}
	t.Fatalf("cluster %q not found in plan", cluster)
	return ClusterPatchPlan{}
}

func conflictKinds(plan ClusterPatchPlan) []ConflictKind {
	kinds := make([]ConflictKind, 0, len(plan.Conflicts))
	for _, conflict := range plan.Conflicts {
		kinds = append(kinds, conflict.Kind)
	}
	sort.Slice(kinds, func(i, j int) bool { return kinds[i] < kinds[j] })
	return kinds
}

// TestPlanImageAndPlainPatchSameContainer verifies that a typed image overrider and a plain jsonpatch
// targeting the same real container pointer are recognized as overlapping writes, while the raw
// patches stay identical to the parseOverrides pipeline.
func TestPlanImageAndPlainPatchSameContainer(t *testing.T) {
	fedObject := generateFedDeploymentWithImage("docker.io/ealen/echo-server:latest")
	clusters := twoClusters()[:1]
	policy := &fedcorev1a1.OverridePolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "op-image"},
		Spec: fedcorev1a1.GenericOverridePolicySpec{
			OverrideRules: []fedcorev1a1.OverrideRule{
				allClusterRule(&fedcorev1a1.Overriders{
					Image: []fedcorev1a1.ImageOverrider{
						{
							ContainerNames: []string{"server"},
							Operations: []fedcorev1a1.Operation{
								{ImageComponent: Tag, Operator: OperatorOverwrite, Value: "v2"},
							},
						},
					},
					JsonPatch: []fedcorev1a1.JsonPatchOverrider{
						{
							Operator: OperatorReplace,
							Path:     "/spec/template/spec/containers/0/image",
							Value:    asJSON("plain.io/server:plain"),
						},
					},
				}),
			},
		},
	}

	plan, err := PlanOverrides(fedObject, []fedcorev1a1.GenericOverridePolicy{policy}, clusters)
	if err != nil {
		t.Fatalf("PlanOverrides returned error: %v", err)
	}

	clusterPlan := findClusterPlan(t, plan, "cluster1")
	assert.Len(t, clusterPlan.Patches, 2)
	imagePatch := clusterPlan.Patches[0]
	plainPatch := clusterPlan.Patches[1]

	assert.Equal(t, "/spec/template/spec/containers/0/image", imagePatch.Patch.Path)
	assert.Equal(t, overriderTypeImage, imagePatch.Source.OverriderType)
	assert.Equal(t, "server", imagePatch.Source.ContainerName)
	assert.Equal(t, []int{0}, imagePatch.Source.OverriderIndexes)
	assert.Equal(t, 0, imagePatch.Index)

	assert.Equal(t, overriderTypeJSONPatch, plainPatch.Source.OverriderType)
	assert.Equal(t, []int{0}, plainPatch.Source.OverriderIndexes)
	assert.Equal(t, 1, plainPatch.Index)

	// The two patches resolve to the same concrete JSON pointer even though their raw overrider field
	// names have nothing in common.
	assert.Contains(t, conflictKinds(clusterPlan), ConflictWriteWrite)
	var writeWrite *PatchConflict
	for i := range clusterPlan.Conflicts {
		if clusterPlan.Conflicts[i].Kind == ConflictWriteWrite {
			writeWrite = &clusterPlan.Conflicts[i]
		}
	}
	if assert.NotNil(t, writeWrite) {
		assert.Equal(t, 0, writeWrite.EarlierPatchIndex)
		assert.Equal(t, "/spec/template/spec/containers/0/image", writeWrite.Path)
		assert.Equal(t, overriderTypeImage, writeWrite.EarlierSource.OverriderType)
	}
	assert.Equal(t, -1, clusterPlan.FailedAtIndex)
	assert.Contains(t, clusterPlan.WritePaths, "/spec/template/spec/containers/0/image")

	assertMatchesReferencePipeline(t, plan, fedObject,
		[]fedcorev1a1.GenericOverridePolicy{policy}, clusters)
}

// TestPlanEnvDuplicateNames verifies env expansion against the real container when env names repeat,
// and that provenance points at the resolved container rather than the raw overrider name only.
func TestPlanEnvDuplicateNames(t *testing.T) {
	fedObject := mustFedObject(t, podWithEnvsTemplate())
	clusters := twoClusters()[:1]
	policy := &fedcorev1a1.OverridePolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "op-env"},
		Spec: fedcorev1a1.GenericOverridePolicySpec{
			OverrideRules: []fedcorev1a1.OverrideRule{
				allClusterRule(&fedcorev1a1.Overriders{
					Envs: []fedcorev1a1.EnvOverrider{
						{
							ContainerName: "server",
							Operator:      OperatorOverwrite,
							Value:         []corev1.EnvVar{{Name: "A", Value: "9"}},
						},
					},
				}),
			},
		},
	}

	plan, err := PlanOverrides(fedObject, []fedcorev1a1.GenericOverridePolicy{policy}, clusters)
	if err != nil {
		t.Fatalf("PlanOverrides returned error: %v", err)
	}
	clusterPlan := findClusterPlan(t, plan, "cluster1")
	if !assert.Len(t, clusterPlan.Patches, 1) {
		return
	}
	patch := clusterPlan.Patches[0]
	assert.Equal(t, "/spec/containers/0/env", patch.Patch.Path)
	assert.Equal(t, overriderTypeEnvs, patch.Source.OverriderType)
	assert.Equal(t, "server", patch.Source.ContainerName)
	assert.Equal(t, []int{0}, patch.Source.OverriderIndexes)

	// Both duplicate A entries are overwritten, matching applyEnvOverrider semantics.
	var envs []corev1.EnvVar
	unmarshalEnvs(t, patch.Patch.Value.Raw, &envs)
	assert.Equal(t, []corev1.EnvVar{
		{Name: "A", Value: "9"},
		{Name: "A", Value: "9"},
		{Name: "B", Value: "3"},
	}, envs)

	assertMatchesReferencePipeline(t, plan, fedObject,
		[]fedcorev1a1.GenericOverridePolicy{policy}, clusters)
}

// TestPlanArrayShifting verifies that array inserts/removes relocate later indexes and that
// parent/child overlaps are interpreted in real execution order.
func TestPlanArrayShifting(t *testing.T) {
	fedObject := mustFedObject(t, widgetTemplate())
	clusters := twoClusters()[:1]
	policy := &fedcorev1a1.OverridePolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "op-array"},
		Spec: fedcorev1a1.GenericOverridePolicySpec{
			OverrideRules: []fedcorev1a1.OverrideRule{
				allClusterRule(&fedcorev1a1.Overriders{
					JsonPatch: []fedcorev1a1.JsonPatchOverrider{
						{Operator: OperatorAdd, Path: "/items/0", Value: asJSON(map[string]string{"id": "z"})},
						{Operator: OperatorReplace, Path: "/items/1/id", Value: asJSON("a2")},
					},
				}),
			},
		},
	}

	plan, err := PlanOverrides(fedObject, []fedcorev1a1.GenericOverridePolicy{policy}, clusters)
	if err != nil {
		t.Fatalf("PlanOverrides returned error: %v", err)
	}
	clusterPlan := findClusterPlan(t, plan, "cluster1")

	// The inserted node occupies its own slot; nothing relocates it further.
	assert.Empty(t, clusterPlan.Patches[0].EffectivePath)
	assert.Equal(t, "/items/1/id", clusterPlan.Patches[1].Patch.Path)

	// The later write lands on the displaced original element, not on the newly inserted node: this is
	// an accurately resolved relocation dependency rather than a write/write conflict on the same node.
	assert.Contains(t, conflictKinds(clusterPlan), ConflictDependency)
	assert.NotContains(t, conflictKinds(clusterPlan), ConflictWriteWrite)
	for _, conflict := range clusterPlan.Conflicts {
		if conflict.Kind == ConflictDependency {
			assert.Equal(t, 0, conflict.EarlierPatchIndex)
			assert.Equal(t, "/items/1/id", conflict.Path)
		}
	}

	items, found, err := unstructured.NestedSlice(clusterPlan.Candidate.Object, "items")
	if err != nil || !found {
		t.Fatalf("candidate items not found: %v", err)
	}
	assert.Equal(t, []interface{}{
		map[string]interface{}{"id": "z"},
		map[string]interface{}{"id": "a2"},
		map[string]interface{}{"id": "b"},
		map[string]interface{}{"id": "c"},
	}, items)

	assertMatchesReferencePipeline(t, plan, fedObject,
		[]fedcorev1a1.GenericOverridePolicy{policy}, clusters)
}

// TestPlanArrayRemovalRelocation verifies removal-driven relocation and the UnknownConflict marker
// when the earlier denoted element no longer exists at apply time.
func TestPlanArrayRemovalRelocation(t *testing.T) {
	fedObject := mustFedObject(t, widgetTemplate())
	clusters := twoClusters()[:1]
	policy := &fedcorev1a1.OverridePolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "op-array-remove"},
		Spec: fedcorev1a1.GenericOverridePolicySpec{
			OverrideRules: []fedcorev1a1.OverrideRule{
				allClusterRule(&fedcorev1a1.Overriders{
					JsonPatch: []fedcorev1a1.JsonPatchOverrider{
						{Operator: OperatorRemove, Path: "/items/1"},
						{Operator: OperatorReplace, Path: "/items/1/id", Value: asJSON("now-c")},
					},
				}),
			},
		},
	}

	plan, err := PlanOverrides(fedObject, []fedcorev1a1.GenericOverridePolicy{policy}, clusters)
	if err != nil {
		t.Fatalf("PlanOverrides returned error: %v", err)
	}
	clusterPlan := findClusterPlan(t, plan, "cluster1")
	// The remove deletes its own target node.
	assert.Equal(t, removedEffectivePathMarker, clusterPlan.Patches[0].EffectivePath)
	// Index 1 after the removal precisely denotes the shifted successor: no unknown verdict needed,
	// only an ordering dependency on the array structure read by the remove.
	assert.Contains(t, conflictKinds(clusterPlan), ConflictDependency)
	assert.NotContains(t, conflictKinds(clusterPlan), ConflictUnknown)
	assert.NotContains(t, conflictKinds(clusterPlan), ConflictWriteWrite)

	// After removing b, index 1 now denotes the shifted c and the replace applies to c exactly as
	// RFC 6902 dictates; the array shrinks by one.
	items, _, err := unstructured.NestedSlice(clusterPlan.Candidate.Object, "items")
	if err != nil {
		t.Fatalf("candidate items not found: %v", err)
	}
	assert.Equal(t, []interface{}{
		map[string]interface{}{"id": "a"},
		map[string]interface{}{"id": "now-c"},
	}, items)

	assertMatchesReferencePipeline(t, plan, fedObject,
		[]fedcorev1a1.GenericOverridePolicy{policy}, clusters)
}

// TestPlanArrayAppendedNodeWriteWrite verifies that writing into a slot introduced by an earlier
// insert is a genuine write/write conflict on the same node identity.
func TestPlanArrayAppendedNodeWriteWrite(t *testing.T) {
	fedObject := mustFedObject(t, widgetTemplate())
	clusters := twoClusters()[:1]
	policy := &fedcorev1a1.OverridePolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "op-array-append"},
		Spec: fedcorev1a1.GenericOverridePolicySpec{
			OverrideRules: []fedcorev1a1.OverrideRule{
				allClusterRule(&fedcorev1a1.Overriders{
					JsonPatch: []fedcorev1a1.JsonPatchOverrider{
						// The template has three items, so "-" inserts at index 3.
						{Operator: OperatorAdd, Path: "/items/-", Value: asJSON(map[string]string{"id": "d"})},
						{Operator: OperatorReplace, Path: "/items/3/id", Value: asJSON("d2")},
					},
				}),
			},
		},
	}

	plan, err := PlanOverrides(fedObject, []fedcorev1a1.GenericOverridePolicy{policy}, clusters)
	if err != nil {
		t.Fatalf("PlanOverrides returned error: %v", err)
	}
	clusterPlan := findClusterPlan(t, plan, "cluster1")
	assert.Contains(t, conflictKinds(clusterPlan), ConflictWriteWrite)
	for _, conflict := range clusterPlan.Conflicts {
		if conflict.Kind == ConflictWriteWrite {
			assert.Equal(t, 0, conflict.EarlierPatchIndex)
			assert.Equal(t, "/items/3/id", conflict.Path)
		}
	}

	assertMatchesReferencePipeline(t, plan, fedObject,
		[]fedcorev1a1.GenericOverridePolicy{policy}, clusters)
}

// TestPlanTestFailure verifies failed test operations are surfaced without mutating the candidate and
// that strict mode rejects them while compatible mode keeps the historical semantics.
func TestPlanTestFailure(t *testing.T) {
	fedObject := mustFedObject(t, widgetTemplate())
	clusters := twoClusters()[:1]
	buildPolicy := func() *fedcorev1a1.OverridePolicy {
		return &fedcorev1a1.OverridePolicy{
			ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "op-test"},
			Spec: fedcorev1a1.GenericOverridePolicySpec{
				OverrideRules: []fedcorev1a1.OverrideRule{
					allClusterRule(&fedcorev1a1.Overriders{
						JsonPatch: []fedcorev1a1.JsonPatchOverrider{
							{Operator: "test", Path: "/a/b", Value: asJSON(int64(2))},
						},
					}),
				},
			},
		}
	}

	// Compatible mode: the failure is recorded on the plan but no error is returned.
	compatiblePlan, err := PlanOverrides(fedObject,
		[]fedcorev1a1.GenericOverridePolicy{buildPolicy()}, clusters)
	if err != nil {
		t.Fatalf("compatible PlanOverrides returned error: %v", err)
	}
	clusterPlan := findClusterPlan(t, compatiblePlan, "cluster1")
	assert.Equal(t, 0, clusterPlan.FailedAtIndex)
	assert.Contains(t, clusterPlan.FailureReason, "test")

	// The candidate stays at the pre-test state and the reference pipeline fails the same way.
	rawCandidate, ok := compatiblePlan.RawCandidateBytes("cluster1")
	assert.True(t, ok)
	templateRaw, err := fedObject.GetSpec().GetTemplateAsUnstructured()
	if err != nil {
		t.Fatalf("template decode failed: %v", err)
	}
	expectedRaw, err := templateRaw.MarshalJSON()
	if err != nil {
		t.Fatalf("template marshal failed: %v", err)
	}
	assert.Equal(t, expectedRaw, rawCandidate)

	// The original pipeline also aborts on the failing test.
	if _, refErr := parseOverrides(buildPolicy(), clusters, fedObject, nil); refErr == nil {
		t.Fatalf("expected reference parseOverrides to fail on test")
	}

	// Strict mode rejects the failure.
	strictPlan, strictErr := PlanOverrides(fedObject,
		[]fedcorev1a1.GenericOverridePolicy{buildPolicy()}, clusters, WithStrictMode())
	var strictConflict *StrictConflictError
	if !errors.As(strictErr, &strictConflict) {
		t.Fatalf("expected StrictConflictError, got %v", strictErr)
	}
	assert.Same(t, strictPlan, strictConflict.Plan)
}

// TestPlanTwoClustersDifferentMatches verifies selector-driven per-cluster plans and provenance.
func TestPlanTwoClustersDifferentMatches(t *testing.T) {
	fedObject := mustFedObject(t, widgetTemplate())
	clusters := twoClusters()
	policy := &fedcorev1a1.OverridePolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "op-multi"},
		Spec: fedcorev1a1.GenericOverridePolicySpec{
			OverrideRules: []fedcorev1a1.OverrideRule{
				allClusterRule(&fedcorev1a1.Overriders{
					JsonPatch: []fedcorev1a1.JsonPatchOverrider{
						{Operator: OperatorAdd, Path: "/metadata/labels/all", Value: asJSON("all")},
					},
				}),
				namedClusterRule("cluster1", &fedcorev1a1.Overriders{
					JsonPatch: []fedcorev1a1.JsonPatchOverrider{
						{Operator: OperatorAdd, Path: "/metadata/labels/c1", Value: asJSON("1")},
					},
				}),
				namedClusterRule("cluster2", &fedcorev1a1.Overriders{
					JsonPatch: []fedcorev1a1.JsonPatchOverrider{
						{Operator: OperatorAdd, Path: "/metadata/labels/c2", Value: asJSON("2")},
					},
				}),
			},
		},
	}

	plan, err := PlanOverrides(fedObject, []fedcorev1a1.GenericOverridePolicy{policy}, clusters)
	if err != nil {
		t.Fatalf("PlanOverrides returned error: %v", err)
	}

	cluster1 := findClusterPlan(t, plan, "cluster1")
	cluster2 := findClusterPlan(t, plan, "cluster2")
	if !assert.Len(t, cluster1.Patches, 2) || !assert.Len(t, cluster2.Patches, 2) {
		return
	}

	assert.Equal(t, 0, cluster1.Patches[0].Source.RuleIndex)
	assert.Equal(t, 1, cluster1.Patches[1].Source.RuleIndex)
	assert.Equal(t, "/metadata/labels/c1", cluster1.Patches[1].Patch.Path)

	assert.Equal(t, 0, cluster2.Patches[0].Source.RuleIndex)
	assert.Equal(t, 2, cluster2.Patches[1].Source.RuleIndex)
	assert.Equal(t, "/metadata/labels/c2", cluster2.Patches[1].Patch.Path)

	assert.Equal(t, "OverridePolicy", cluster1.Patches[0].Source.PolicyKind)
	assert.Equal(t, "default/op-multi", cluster1.Patches[0].Source.PolicyKey)

	assertMatchesReferencePipeline(t, plan, fedObject,
		[]fedcorev1a1.GenericOverridePolicy{policy}, clusters)
}

// TestPlanPolicyOrderPreserved verifies ClusterOverridePolicy-before-OverridePolicy ordering is kept
// and identical to mergeOverrides.
func TestPlanPolicyOrderPreserved(t *testing.T) {
	fedObject := mustFedObject(t, widgetTemplate())
	clusters := twoClusters()[:1]
	clusterPolicy := &fedcorev1a1.ClusterOverridePolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "cop-1"},
		Spec: fedcorev1a1.GenericOverridePolicySpec{
			OverrideRules: []fedcorev1a1.OverrideRule{
				allClusterRule(&fedcorev1a1.Overriders{
					JsonPatch: []fedcorev1a1.JsonPatchOverrider{
						{Operator: OperatorAdd, Path: "/metadata/labels/from-cop", Value: asJSON("1")},
					},
				}),
			},
		},
	}
	namespacedPolicy := &fedcorev1a1.OverridePolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "op-1"},
		Spec: fedcorev1a1.GenericOverridePolicySpec{
			OverrideRules: []fedcorev1a1.OverrideRule{
				allClusterRule(&fedcorev1a1.Overriders{
					JsonPatch: []fedcorev1a1.JsonPatchOverrider{
						{Operator: OperatorAdd, Path: "/metadata/labels/from-op", Value: asJSON("2")},
					},
				}),
			},
		},
	}
	policies := []fedcorev1a1.GenericOverridePolicy{clusterPolicy, namespacedPolicy}

	plan, err := PlanOverrides(fedObject, policies, clusters)
	if err != nil {
		t.Fatalf("PlanOverrides returned error: %v", err)
	}
	clusterPlan := findClusterPlan(t, plan, "cluster1")
	if !assert.Len(t, clusterPlan.Patches, 2) {
		return
	}
	assert.Equal(t, "ClusterOverridePolicy", clusterPlan.Patches[0].Source.PolicyKind)
	assert.Equal(t, "cop-1", clusterPlan.Patches[0].Source.PolicyKey)
	assert.Equal(t, "OverridePolicy", clusterPlan.Patches[1].Source.PolicyKind)
	assert.Equal(t, "default/op-1", clusterPlan.Patches[1].Source.PolicyKey)

	assertMatchesReferencePipeline(t, plan, fedObject, policies, clusters)
}

// TestPlanFinalizerPatchesDeferred verifies the finalizer filtering rule is preserved: finalizer
// patches remain in the raw list but never drive the candidate object.
func TestPlanFinalizerPatchesDeferred(t *testing.T) {
	fedObject := mustFedObject(t, widgetTemplate())
	clusters := twoClusters()[:1]
	policy := &fedcorev1a1.OverridePolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "op-finalizer"},
		Spec: fedcorev1a1.GenericOverridePolicySpec{
			OverrideRules: []fedcorev1a1.OverrideRule{
				allClusterRule(&fedcorev1a1.Overriders{
					JsonPatch: []fedcorev1a1.JsonPatchOverrider{
						// The deferred finalizer patch sits between two ordinary patches and must not
						// degrade the analysis or the candidate of the patch that follows it.
						{Operator: OperatorAdd, Path: "/metadata/labels/kept", Value: asJSON("yes")},
						{Operator: OperatorAdd, Path: "/metadata/finalizers/-", Value: asJSON("foo.bar.com/test")},
						{Operator: OperatorAdd, Path: "/metadata/labels/after", Value: asJSON("yes")},
					},
				}),
			},
		},
	}

	plan, err := PlanOverrides(fedObject, []fedcorev1a1.GenericOverridePolicy{policy}, clusters)
	if err != nil {
		t.Fatalf("PlanOverrides returned error: %v", err)
	}
	clusterPlan := findClusterPlan(t, plan, "cluster1")
	if !assert.Len(t, clusterPlan.Patches, 3) {
		return
	}
	assert.False(t, clusterPlan.Patches[0].Deferred)
	assert.True(t, clusterPlan.Patches[1].Deferred)
	assert.False(t, clusterPlan.Patches[2].Deferred)
	assert.Equal(t, -1, clusterPlan.FailedAtIndex)
	assert.NotContains(t, conflictKinds(clusterPlan), ConflictUnknown)
	_, hasFinalizers, err := unstructured.NestedSlice(clusterPlan.Candidate.Object, "metadata", "finalizers")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assert.False(t, hasFinalizers)
	after, _, err := unstructured.NestedString(clusterPlan.Candidate.Object, "metadata", "labels", "after")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assert.Equal(t, "yes", after)

	assertMatchesReferencePipeline(t, plan, fedObject,
		[]fedcorev1a1.GenericOverridePolicy{policy}, clusters)
}

// TestPlanMoveWithoutFromIsUnknown verifies undecidable move/copy dependencies become UnknownConflict.
func TestPlanMoveWithoutFromIsUnknown(t *testing.T) {
	fedObject := mustFedObject(t, widgetTemplate())
	clusters := twoClusters()[:1]
	policy := &fedcorev1a1.OverridePolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "op-move"},
		Spec: fedcorev1a1.GenericOverridePolicySpec{
			OverrideRules: []fedcorev1a1.OverrideRule{
				allClusterRule(&fedcorev1a1.Overriders{
					JsonPatch: []fedcorev1a1.JsonPatchOverrider{
						{Operator: OperatorReplace, Path: "/a/b", Value: asJSON(int64(2))},
						// The OverridePatch schema cannot carry "from", so precise move analysis is impossible.
						{Operator: "move", Path: "/a/b"},
					},
				}),
			},
		},
	}

	plan, err := PlanOverrides(fedObject, []fedcorev1a1.GenericOverridePolicy{policy}, clusters)
	if err != nil {
		t.Fatalf("PlanOverrides returned error: %v", err)
	}
	clusterPlan := findClusterPlan(t, plan, "cluster1")
	assert.Contains(t, conflictKinds(clusterPlan), ConflictUnknown)
	assert.Equal(t, 1, clusterPlan.FailedAtIndex)
}

// TestPlanApplyRevalidation verifies plan binding checks and replay semantics, without any API writes.
func TestPlanApplyRevalidation(t *testing.T) {
	fedObject := mustFedObject(t, widgetTemplate())
	clusters := twoClusters()[:1]
	policy := &fedcorev1a1.OverridePolicy{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       "default",
			Name:            "op-apply",
			UID:             types.UID("uid-policy"),
			ResourceVersion: "7",
		},
		Spec: fedcorev1a1.GenericOverridePolicySpec{
			OverrideRules: []fedcorev1a1.OverrideRule{
				allClusterRule(&fedcorev1a1.Overriders{
					JsonPatch: []fedcorev1a1.JsonPatchOverrider{
						{Operator: OperatorReplace, Path: "/a/b", Value: asJSON(int64(42))},
					},
				}),
			},
		},
	}
	policies := []fedcorev1a1.GenericOverridePolicy{policy}

	plan, err := PlanOverrides(fedObject, policies, clusters)
	if err != nil {
		t.Fatalf("PlanOverrides returned error: %v", err)
	}

	applied, err := plan.Apply(fedObject, policies, clusters)
	if err != nil {
		t.Fatalf("Apply returned error: %v", err)
	}
	assert.Len(t, applied, 1)
	if assert.Len(t, applied["cluster1"], 1) {
		assert.Equal(t, "/a/b", applied["cluster1"][0].Path)
	}

	// Stale resource version.
	staleObject := fedObject.DeepCopy()
	staleObject.ResourceVersion = "101"
	_, err = plan.Apply(staleObject, policies, clusters)
	assert.ErrorIs(t, err, ErrPlanBindingMismatch)

	// Stale template.
	changedTemplateObject := fedObject.DeepCopy()
	changedTemplateObject.Spec.Template = asJSON(map[string]interface{}{"changed": true})
	_, err = plan.Apply(changedTemplateObject, policies, clusters)
	assert.ErrorIs(t, err, ErrPlanBindingMismatch)

	// Stale policy digest.
	changedPolicy := policy.DeepCopy()
	changedPolicy.Spec.OverrideRules[0].Overriders.JsonPatch[0].Value = asJSON(int64(43))
	_, err = plan.Apply(fedObject, []fedcorev1a1.GenericOverridePolicy{changedPolicy}, clusters)
	assert.ErrorIs(t, err, ErrPlanBindingMismatch)

	// Stale cluster snapshot.
	relabelledCluster := &fedcorev1a1.FederatedCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "cluster1", Labels: map[string]string{"zone": "east"}},
	}
	_, err = plan.Apply(fedObject, policies, []*fedcorev1a1.FederatedCluster{relabelledCluster})
	assert.ErrorIs(t, err, ErrPlanBindingMismatch)

	// Replay with identical inputs succeeds again, proving determinism.
	again, err := plan.Apply(fedObject, policies, clusters)
	if err != nil {
		t.Fatalf("second Apply returned error: %v", err)
	}
	assert.True(t, reflect.DeepEqual(applied, again))
}

// TestPlanDoesNotMutateInputs verifies a failed or successful plan leaves inputs untouched.
func TestPlanDoesNotMutateInputs(t *testing.T) {
	fedObject := mustFedObject(t, widgetTemplate())
	clusters := twoClusters()
	policy := &fedcorev1a1.OverridePolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "op-immutable"},
		Spec: fedcorev1a1.GenericOverridePolicySpec{
			OverrideRules: []fedcorev1a1.OverrideRule{
				allClusterRule(&fedcorev1a1.Overriders{
					JsonPatch: []fedcorev1a1.JsonPatchOverrider{
						{Operator: "test", Path: "/a/b", Value: asJSON(int64(99))},
					},
				}),
			},
		},
	}

	objectBefore := fedObject.DeepCopy()
	policyBefore := policy.DeepCopy()
	clustersBefore := deepCopyClusters(clusters)

	plan, err := PlanOverrides(fedObject, []fedcorev1a1.GenericOverridePolicy{policy}, clusters)
	if err != nil {
		t.Fatalf("PlanOverrides returned error: %v", err)
	}
	if assert.Len(t, plan.ClusterPlans, 2) {
		for _, clusterPlan := range plan.ClusterPlans {
			assert.GreaterOrEqual(t, clusterPlan.FailedAtIndex, 0)
		}
	}

	assert.True(t, reflect.DeepEqual(objectBefore, fedObject), "fedObject was mutated")
	assert.True(t, reflect.DeepEqual(policyBefore, policy), "policy was mutated")
	assert.True(t, reflect.DeepEqual(clustersBefore, clusters), "clusters were mutated")
}

// TestPlanSecretRedaction verifies secret values are redacted in the exported candidate while the raw
// bytes used for accounting keep the real values.
func TestPlanSecretRedaction(t *testing.T) {
	secretTemplate := map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "Secret",
		"metadata":   map[string]interface{}{"name": "my-secret"},
		"type":       "Opaque",
		"data": map[string]interface{}{
			"password": "cGFzc3dvcmQ=",
		},
		"stringData": map[string]interface{}{
			"token": "super-secret-token",
		},
	}
	fedObject := mustFedObject(t, secretTemplate)
	clusters := twoClusters()[:1]

	plan, err := PlanOverrides(fedObject, nil, clusters)
	if err != nil {
		t.Fatalf("PlanOverrides returned error: %v", err)
	}
	clusterPlan := findClusterPlan(t, plan, "cluster1")
	assert.True(t, clusterPlan.Redacted)

	data, _, err := unstructured.NestedMap(clusterPlan.Candidate.Object, "data")
	if err != nil {
		t.Fatalf("data lookup failed: %v", err)
	}
	assert.Equal(t, redactionMarker, data["password"])
	stringData, _, err := unstructured.NestedMap(clusterPlan.Candidate.Object, "stringData")
	if err != nil {
		t.Fatalf("stringData lookup failed: %v", err)
	}
	assert.Equal(t, redactionMarker, stringData["token"])

	raw, ok := plan.RawCandidateBytes("cluster1")
	assert.True(t, ok)
	assert.Contains(t, string(raw), "cGFzc3dvcmQ=")
	assert.NotContains(t, marshalForComparison(t, clusterPlan.Candidate), "super-secret-token")
}

// TestPlanLimits verifies the byte and count guards.
func TestPlanLimits(t *testing.T) {
	fedObject := mustFedObject(t, widgetTemplate())
	clusters := twoClusters()[:1]
	policy := &fedcorev1a1.OverridePolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "op-limits"},
		Spec: fedcorev1a1.GenericOverridePolicySpec{
			OverrideRules: []fedcorev1a1.OverrideRule{
				allClusterRule(&fedcorev1a1.Overriders{
					JsonPatch: []fedcorev1a1.JsonPatchOverrider{
						{Operator: OperatorAdd, Path: "/metadata/labels/a", Value: asJSON("1")},
						{Operator: OperatorAdd, Path: "/metadata/labels/b", Value: asJSON("2")},
					},
				}),
			},
		},
	}
	policies := []fedcorev1a1.GenericOverridePolicy{policy}

	_, err := PlanOverrides(fedObject, policies, clusters, WithPlanLimits(PlanLimits{MaxPatches: 1}))
	assert.ErrorIs(t, err, ErrPlanLimitExceeded)

	_, err = PlanOverrides(fedObject, policies, clusters, WithPlanLimits(PlanLimits{MaxCandidateBytes: 8}))
	assert.ErrorIs(t, err, ErrPlanLimitExceeded)

	_, err = PlanOverrides(fedObject, policies, clusters,
		WithPlanLimits(PlanLimits{MaxPathBytes: 4}))
	assert.ErrorIs(t, err, ErrPlanLimitExceeded)
}

// TestPlanStrictModeRejectsOverlappingWrites verifies the opt-in strict mode while the default stays
// compatible.
func TestPlanStrictModeRejectsOverlappingWrites(t *testing.T) {
	fedObject := generateFedDeploymentWithImage("docker.io/ealen/echo-server:latest")
	clusters := twoClusters()[:1]
	buildPolicy := func() *fedcorev1a1.OverridePolicy {
		return &fedcorev1a1.OverridePolicy{
			ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "op-strict"},
			Spec: fedcorev1a1.GenericOverridePolicySpec{
				OverrideRules: []fedcorev1a1.OverrideRule{
					allClusterRule(&fedcorev1a1.Overriders{
						Image: []fedcorev1a1.ImageOverrider{
							{
								ContainerNames: []string{"server"},
								Operations: []fedcorev1a1.Operation{
									{ImageComponent: Tag, Operator: OperatorOverwrite, Value: "v2"},
								},
							},
						},
						JsonPatch: []fedcorev1a1.JsonPatchOverrider{
							{
								Operator: OperatorReplace,
								Path:     "/spec/template/spec/containers/0/image",
								Value:    asJSON("plain.io/server:plain"),
							},
						},
					}),
				},
			},
		}
	}
	policies := []fedcorev1a1.GenericOverridePolicy{buildPolicy()}

	compatiblePlan, err := PlanOverrides(fedObject, policies, clusters)
	if err != nil {
		t.Fatalf("compatible plan should not error: %v", err)
	}
	assert.Contains(t, conflictKinds(findClusterPlan(t, compatiblePlan, "cluster1")), ConflictWriteWrite)

	_, err = PlanOverrides(fedObject, policies, clusters, WithStrictMode())
	var strictErr *StrictConflictError
	if !errors.As(err, &strictErr) {
		t.Fatalf("expected StrictConflictError, got %v", err)
	}
	assert.NotEmpty(t, strictErr.Reasons)
}

// TestPlanAllOverriderTypesMatchReference exercises every typed segment in a single rule and verifies
// that the ordered raw patches and candidate bytes are identical to the parseOverrides pipeline.
func TestPlanAllOverriderTypesMatchReference(t *testing.T) {
	image := "docker.io/ealen/echo-server:latest@sha256:bbbbf56b44807c64d294e6c8059b479f35350b454492398225034174808d1726"
	fedObject := generateFedObjWithPodWithTwoNormalAndTwoInit(image)
	clusters := twoClusters()
	policy := &fedcorev1a1.OverridePolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "op-all"},
		Spec: fedcorev1a1.GenericOverridePolicySpec{
			OverrideRules: []fedcorev1a1.OverrideRule{
				allClusterRule(&fedcorev1a1.Overriders{
					Image: []fedcorev1a1.ImageOverrider{
						{
							ContainerNames: []string{"server-1"},
							Operations: []fedcorev1a1.Operation{
								{ImageComponent: Tag, Operator: OperatorOverwrite, Value: "v9"},
							},
						},
					},
					Command: []fedcorev1a1.EntrypointOverrider{
						generateEntrypointOverrider("server-1", OperatorAppend, "/bin/bash", "sleep 15"),
					},
					Args: []fedcorev1a1.EntrypointOverrider{
						generateEntrypointOverrider("server-1", OperatorOverwrite, "-c", "-d"),
					},
					Envs: []fedcorev1a1.EnvOverrider{
						generateEnvOverrider("server-1", OperatorAddIfAbsent, corev1.EnvVar{Name: "NEW", Value: "v"}),
					},
					Annotations: []fedcorev1a1.StringMapOverrider{
						generateStringMapOverrider(OperatorAddIfAbsent, map[string]string{"ann": "v"}),
					},
					Labels: []fedcorev1a1.StringMapOverrider{
						generateStringMapOverrider(OperatorOverwrite, map[string]string{"lbl": "v"}),
					},
					JsonPatch: []fedcorev1a1.JsonPatchOverrider{
						{Operator: OperatorAdd, Path: "/metadata/labels/plain", Value: asJSON("1")},
					},
				}),
			},
		},
	}
	policies := []fedcorev1a1.GenericOverridePolicy{policy}

	plan, err := PlanOverrides(fedObject, policies, clusters)
	if err != nil {
		t.Fatalf("PlanOverrides returned error: %v", err)
	}

	// Every segment contributes and carries accurate typed provenance.
	cluster1 := findClusterPlan(t, plan, "cluster1")
	seenTypes := make(map[string]bool)
	for _, patch := range cluster1.Patches {
		seenTypes[patch.Source.OverriderType] = true
		assert.Equal(t, "OverridePolicy", patch.Source.PolicyKind)
		assert.Equal(t, 0, patch.Source.RuleIndex)
		assert.NotEmpty(t, patch.Source.OverriderIndexes)
	}
	for _, expected := range []string{
		overriderTypeImage, overriderTypeCommand, overriderTypeArgs, overriderTypeEnvs,
		overriderTypeAnnotations, overriderTypeLabels, overriderTypeJSONPatch,
	} {
		assert.True(t, seenTypes[expected], "missing segment %s", expected)
	}
	assert.Equal(t, -1, cluster1.FailedAtIndex)

	assertMatchesReferencePipeline(t, plan, fedObject, policies, clusters)
}

// TestPlanBindingDigests verifies the plan carries UID, resourceVersion and policy/cluster digests.
func TestPlanBindingDigests(t *testing.T) {
	fedObject := mustFedObject(t, widgetTemplate())
	clusters := twoClusters()
	policy := &fedcorev1a1.OverridePolicy{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       "default",
			Name:            "op-digest",
			UID:             types.UID("uid-policy"),
			ResourceVersion: "9",
		},
		Spec: fedcorev1a1.GenericOverridePolicySpec{
			OverrideRules: []fedcorev1a1.OverrideRule{
				allClusterRule(&fedcorev1a1.Overriders{
					JsonPatch: []fedcorev1a1.JsonPatchOverrider{
						{Operator: OperatorAdd, Path: "/metadata/labels/x", Value: asJSON("y")},
					},
				}),
			},
		},
	}

	plan, err := PlanOverrides(fedObject, []fedcorev1a1.GenericOverridePolicy{policy}, clusters)
	if err != nil {
		t.Fatalf("PlanOverrides returned error: %v", err)
	}
	assert.Equal(t, types.UID("uid-fed-object"), plan.FedObjectUID)
	assert.Equal(t, "100", plan.FedObjectResourceVersion)
	assert.Len(t, plan.TemplateDigest, 64)
	if assert.Len(t, plan.Policies, 1) {
		assert.Equal(t, "OverridePolicy", plan.Policies[0].Kind)
		assert.Equal(t, "default/op-digest", plan.Policies[0].Key)
		assert.Equal(t, types.UID("uid-policy"), plan.Policies[0].UID)
		assert.Len(t, plan.Policies[0].Digest, 64)
	}
	assert.Len(t, plan.Clusters, 2)
	for _, binding := range plan.Clusters {
		assert.Len(t, binding.Digest, 64)
	}
	assert.False(t, plan.CreatedAt.IsZero())
}

func unmarshalEnvs(t *testing.T, raw []byte, target *[]corev1.EnvVar) {
	t.Helper()
	if err := jsonutil.Unmarshal(raw, target); err != nil {
		t.Fatalf("failed to unmarshal env patch value: %v", err)
	}
}

func marshalForComparison(t *testing.T, obj *unstructured.Unstructured) string {
	t.Helper()
	raw, err := obj.MarshalJSON()
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	return string(raw)
}

func deepCopyClusters(clusters []*fedcorev1a1.FederatedCluster) []*fedcorev1a1.FederatedCluster {
	result := make([]*fedcorev1a1.FederatedCluster, 0, len(clusters))
	for _, cluster := range clusters {
		result = append(result, cluster.DeepCopy())
	}
	return result
}
