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
	"sync"
	"testing"
	"time"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"

	fedcorev1a1 "github.com/kubewharf/kubeadmiral/pkg/apis/core/v1alpha1"
	"github.com/kubewharf/kubeadmiral/pkg/controllers/common"
	"github.com/kubewharf/kubeadmiral/pkg/util/managedlabel"
)

const (
	testNamespace  = "default"
	testObjectName = "cm"
	testFedObjName = "cm-fed"
	testClusterA   = "cluster-a"
	testClusterB   = "cluster-b"
	testUIDA       = types.UID("uid-cluster-a")
	testUIDB       = types.UID("uid-cluster-b")
	testFedUID     = types.UID("uid-fed-object-1")
	testSigningKey = "unit-test-signing-key"
)

var testSourceGVK = schema.GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMap"}

// fakeClock is a manually controlled clock shared by manager and test code.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// fakeStateReader is an in-memory, strictly read-only StateReader.
type fakeStateReader struct {
	mu sync.Mutex

	fedObjects map[string]fedcorev1a1.GenericFederatedObject
	pvStatuses map[string]*fedcorev1a1.PropagatedVersionStatus
	clusters   map[string]*fedcorev1a1.FederatedCluster

	fedObjectErr error
	pvErr        error
	clusterErr   error

	fedObjectReads int
	pvReads        int
	clusterReads   int
}

func newFakeStateReader() *fakeStateReader {
	return &fakeStateReader{
		fedObjects: make(map[string]fedcorev1a1.GenericFederatedObject),
		pvStatuses: make(map[string]*fedcorev1a1.PropagatedVersionStatus),
		clusters:   make(map[string]*fedcorev1a1.FederatedCluster),
	}
}

func (r *fakeStateReader) GetFederatedObject(_ context.Context, ref ObjectRef) (fedcorev1a1.GenericFederatedObject, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.fedObjectReads++
	if r.fedObjectErr != nil {
		return nil, r.fedObjectErr
	}
	return r.fedObjects[ref.String()], nil
}

func (r *fakeStateReader) GetPropagatedVersion(_ context.Context, ref ObjectRef) (*fedcorev1a1.PropagatedVersionStatus, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pvReads++
	if r.pvErr != nil {
		return nil, r.pvErr
	}
	return r.pvStatuses[ref.String()], nil
}

func (r *fakeStateReader) GetFederatedCluster(_ context.Context, clusterName string) (*fedcorev1a1.FederatedCluster, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.clusterReads++
	if r.clusterErr != nil {
		return nil, r.clusterErr
	}
	return r.clusters[clusterName], nil
}

func (r *fakeStateReader) setFedObject(obj fedcorev1a1.GenericFederatedObject) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.fedObjects[common.NewQualifiedName(obj).String()] = obj
}

func (r *fakeStateReader) setPVStatus(ref ObjectRef, status *fedcorev1a1.PropagatedVersionStatus) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pvStatuses[ref.String()] = status
}

func (r *fakeStateReader) setCluster(cluster *fedcorev1a1.FederatedCluster) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.clusters[cluster.Name] = cluster
}

// fakeMemberObjectObserver is an in-memory, strictly read-only member observer.
type fakeMemberObjectObserver struct {
	mu sync.Mutex
	// objects maps "clusterName/namespace/name" to the observed object.
	objects map[string]*unstructured.Unstructured
	err     error
	calls   int
}

func newFakeMemberObjectObserver() *fakeMemberObjectObserver {
	return &fakeMemberObjectObserver{objects: make(map[string]*unstructured.Unstructured)}
}

func memberObjectKey(clusterName string, name common.QualifiedName) string {
	return clusterName + "/" + name.String()
}

func (o *fakeMemberObjectObserver) ObserveMemberObject(
	_ context.Context,
	clusterName string,
	_ schema.GroupVersionKind,
	targetName common.QualifiedName,
) (*unstructured.Unstructured, bool, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.calls++
	if o.err != nil {
		return nil, false, o.err
	}
	obj, ok := o.objects[memberObjectKey(clusterName, targetName)]
	if !ok {
		return nil, false, nil
	}
	return obj, true, nil
}

func (o *fakeMemberObjectObserver) put(clusterName string, obj *unstructured.Unstructured) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.objects[memberObjectKey(clusterName, common.NewQualifiedName(obj))] = obj
}

func (o *fakeMemberObjectObserver) callCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.calls
}

// --- object builders ---

func newTemplate(generation int64) *unstructured.Unstructured {
	template := &unstructured.Unstructured{}
	template.SetAPIVersion("v1")
	template.SetKind("ConfigMap")
	template.SetNamespace(testNamespace)
	template.SetName(testObjectName)
	template.SetGeneration(generation)
	_ = unstructured.SetNestedStringMap(template.Object, map[string]string{"key": "value"}, "data")
	return template
}

func newFedObject(
	t *testing.T,
	uid types.UID,
	generation int64,
	template *unstructured.Unstructured,
	overrides []fedcorev1a1.OverrideWithController,
	placementClusters ...string,
) *fedcorev1a1.FederatedObject {
	t.Helper()
	raw, err := template.MarshalJSON()
	if err != nil {
		t.Fatalf("marshal template: %v", err)
	}
	placement := make([]fedcorev1a1.ClusterReference, 0, len(placementClusters))
	for _, clusterName := range placementClusters {
		placement = append(placement, fedcorev1a1.ClusterReference{Cluster: clusterName})
	}
	return &fedcorev1a1.FederatedObject{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:  testNamespace,
			Name:       testFedObjName,
			UID:        uid,
			Generation: generation,
		},
		Spec: fedcorev1a1.GenericFederatedObjectSpec{
			Template:   apiextensionsv1.JSON{Raw: raw},
			Overrides:  overrides,
			Placements: []fedcorev1a1.PlacementWithController{{Controller: "test", Placement: placement}},
		},
	}
}

func withClusterStatus(
	obj *fedcorev1a1.FederatedObject,
	clusterName string,
	status fedcorev1a1.PropagationStatusType,
	generation int64,
) *fedcorev1a1.FederatedObject {
	obj.Status.Clusters = append(obj.Status.Clusters, fedcorev1a1.PropagationStatus{
		Cluster:                clusterName,
		Status:                 status,
		LastObservedGeneration: generation,
	})
	return obj
}

func newCluster(name string, uid types.UID) *fedcorev1a1.FederatedCluster {
	return &fedcorev1a1.FederatedCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, UID: uid},
	}
}

func newMemberObject(generation int64, resourceVersion string, managed bool) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{}
	obj.SetAPIVersion("v1")
	obj.SetKind("ConfigMap")
	obj.SetNamespace(testNamespace)
	obj.SetName(testObjectName)
	if generation != 0 {
		obj.SetGeneration(generation)
	}
	obj.SetResourceVersion(resourceVersion)
	if managed {
		managedlabel.AddManagedLabel(obj)
	}
	return obj
}

func withSourceGeneration(obj *unstructured.Unstructured, generation int64) *unstructured.Unstructured {
	annotations := obj.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string)
	}
	annotations[common.SourceGenerationAnnotation] = fmt.Sprintf("%d", generation)
	obj.SetAnnotations(annotations)
	return obj
}

func newPVStatus(templateDigest, overrideDigest string, clusterVersions map[string]string) *fedcorev1a1.PropagatedVersionStatus {
	status := &fedcorev1a1.PropagatedVersionStatus{
		TemplateVersion: templateDigest,
		OverrideVersion: overrideDigest,
	}
	for clusterName, version := range clusterVersions {
		status.ClusterVersions = append(status.ClusterVersions, fedcorev1a1.ClusterObjectVersion{
			ClusterName: clusterName,
			Version:     version,
		})
	}
	return status
}

// --- manager / token helpers ---

func testLimits(requery time.Duration) Limits {
	limits := DefaultLimits()
	limits.RequeryInterval = requery
	limits.SweepInterval = time.Hour
	limits.DefaultTTL = 10 * time.Second
	limits.MaxTTL = time.Minute
	return limits
}

func newTestManager(reader StateReader, observer MemberObjectObserver, clock *fakeClock, limits Limits) *Manager {
	manager := NewManager("test", []byte(testSigningKey), reader, observer, limits, klog.Background())
	manager.now = clock.Now
	return manager
}

func defaultRef() ObjectRef {
	return ObjectRef{Namespace: testNamespace, Name: testFedObjName}
}

func issueTokenFor(
	t *testing.T,
	manager *Manager,
	fedObject *fedcorev1a1.FederatedObject,
	targets ...PropagationTarget,
) (string, *PropagationBarrier) {
	t.Helper()
	template, err := fedObject.GetSpec().GetTemplateAsUnstructured()
	if err != nil {
		t.Fatalf("get template: %v", err)
	}
	templateDigest, err := DigestTemplate(template)
	if err != nil {
		t.Fatalf("digest template: %v", err)
	}
	overrideDigest, err := DigestOverrides(fedObject.GetSpec().Overrides)
	if err != nil {
		t.Fatalf("digest overrides: %v", err)
	}
	opaque, barrier, err := manager.Issue(IssueTokenInput{
		ObjectRef:      defaultRef(),
		FederatedGVK:   common.FederatedObjectGVK,
		SourceGVK:      testSourceGVK,
		FederatedUID:   fedObject.UID,
		Generation:     fedObject.Generation,
		TemplateDigest: templateDigest,
		OverrideDigest: overrideDigest,
		Targets:        targets,
	})
	if err != nil {
		t.Fatalf("issue token: %v", err)
	}
	return opaque, barrier
}

func targetFromCluster(cluster *fedcorev1a1.FederatedCluster) PropagationTarget {
	return PropagationTarget{ClusterName: cluster.Name, ClusterUID: cluster.UID}
}

// findResult locates a target result by cluster name.
func findResult(t *testing.T, outcome *WaitOutcome, clusterName string) TargetResult {
	t.Helper()
	for _, result := range outcome.Results {
		if result.ClusterName == clusterName {
			return result
		}
	}
	t.Fatalf("cluster %q not found in outcome: %+v", clusterName, outcome)
	return TargetResult{}
}

func assertState(t *testing.T, result TargetResult, expected TargetState) {
	t.Helper()
	if result.State != expected {
		t.Fatalf("expected state %s, got %s (reason=%s message=%s)",
			expected, result.State, result.Reason, result.Message)
	}
}
