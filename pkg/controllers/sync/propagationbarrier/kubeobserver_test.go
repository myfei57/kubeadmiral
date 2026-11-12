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
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	fedcorev1a1 "github.com/kubewharf/kubeadmiral/pkg/apis/core/v1alpha1"
	"github.com/kubewharf/kubeadmiral/pkg/controllers/common"
	"github.com/kubewharf/kubeadmiral/pkg/util/informermanager"
)

// --- narrow fakes for the production observer ---

type fakeFTCManager struct {
	informermanager.FederatedTypeConfigManager
	ftc *fedcorev1a1.FederatedTypeConfig
}

func (f *fakeFTCManager) GetResourceFTC(gvk schema.GroupVersionKind) (*fedcorev1a1.FederatedTypeConfig, bool) {
	if f.ftc != nil && f.ftc.GetSourceTypeGVK() == gvk {
		return f.ftc, true
	}
	return nil, false
}

type fakeFedInformerManager struct {
	informermanager.FederatedInformerManager

	lister       cache.GenericLister
	listerExists bool
	synced       bool

	dynamicClient dynamic.Interface
	clientExists  bool
}

func (f *fakeFedInformerManager) GetResourceLister(
	_ schema.GroupVersionKind, _ string,
) (cache.GenericLister, cache.InformerSynced, bool) {
	return f.lister, func() bool { return f.synced }, f.listerExists
}

func (f *fakeFedInformerManager) GetClusterDynamicClient(_ string) (dynamic.Interface, bool) {
	return f.dynamicClient, f.clientExists
}

type fakeGenericLister struct {
	obj runtime.Object
	err error
}

func (l *fakeGenericLister) List(_ labels.Selector) ([]runtime.Object, error) {
	if l.err != nil {
		return nil, l.err
	}
	return []runtime.Object{l.obj}, nil
}

func (l *fakeGenericLister) Get(_ string) (runtime.Object, error) {
	return l.obj, l.err
}

func (l *fakeGenericLister) ByNamespace(_ string) cache.GenericNamespaceLister {
	return nil
}

func testFTC() *fedcorev1a1.FederatedTypeConfig {
	return &fedcorev1a1.FederatedTypeConfig{
		Spec: fedcorev1a1.FederatedTypeConfigSpec{
			SourceType: fedcorev1a1.APIResource{
				Group:      "",
				Version:    "v1",
				Kind:       "ConfigMap",
				PluralName: "configmaps",
			},
		},
	}
}

func TestKubeMemberObjectObserverSyncedLister(t *testing.T) {
	observer := &KubeMemberObjectObserver{
		ftcManager:         &fakeFTCManager{ftc: testFTC()},
		fedInformerManager: &fakeFedInformerManager{listerExists: true, synced: true},
	}
	targetName := common.QualifiedName{Namespace: testNamespace, Name: testObjectName}

	t.Run("managed object observed", func(t *testing.T) {
		managed := newMemberObject(2, "rv-1", true)
		observer.fedInformerManager.(*fakeFedInformerManager).lister = &fakeGenericLister{obj: managed}
		obj, found, err := observer.ObserveMemberObject(context.Background(), testClusterA, testSourceGVK, targetName)
		if err != nil || !found || obj == nil {
			t.Fatalf("expected managed object, got found=%v err=%v", found, err)
		}
	})

	t.Run("unmanaged object is treated as absent", func(t *testing.T) {
		unmanaged := newMemberObject(2, "rv-1", false)
		observer.fedInformerManager.(*fakeFedInformerManager).lister = &fakeGenericLister{obj: unmanaged}
		obj, found, err := observer.ObserveMemberObject(context.Background(), testClusterA, testSourceGVK, targetName)
		if err != nil || found || obj != nil {
			t.Fatalf("expected absent unmanaged object, got found=%v obj=%v err=%v", found, obj, err)
		}
	})

	t.Run("lister not found is treated as absent", func(t *testing.T) {
		observer.fedInformerManager.(*fakeFedInformerManager).lister = &fakeGenericLister{
			err: apierrors.NewNotFound(schema.GroupResource{Resource: "configmaps"}, testObjectName),
		}
		_, found, err := observer.ObserveMemberObject(context.Background(), testClusterA, testSourceGVK, targetName)
		if err != nil || found {
			t.Fatalf("expected absent object on NotFound, got found=%v err=%v", found, err)
		}
	})
}

func TestKubeMemberObjectObserverDynamicFallback(t *testing.T) {
	targetName := common.QualifiedName{Namespace: testNamespace, Name: testObjectName}

	t.Run("direct GET covers informer lag", func(t *testing.T) {
		managed := newMemberObject(2, "rv-1", true)
		dynamicClient := dynamicfake.NewSimpleDynamicClient(scheme.Scheme, managed)
		observer := &KubeMemberObjectObserver{
			ftcManager: &fakeFTCManager{ftc: testFTC()},
			fedInformerManager: &fakeFedInformerManager{
				listerExists: true, synced: false, clientExists: true, dynamicClient: dynamicClient,
			},
		}
		obj, found, err := observer.ObserveMemberObject(context.Background(), testClusterA, testSourceGVK, targetName)
		if err != nil || !found || obj == nil {
			t.Fatalf("expected fallback GET to find the managed object, got found=%v err=%v", found, err)
		}
	})

	t.Run("missing client is an error", func(t *testing.T) {
		observer := &KubeMemberObjectObserver{
			ftcManager:         &fakeFTCManager{ftc: testFTC()},
			fedInformerManager: &fakeFedInformerManager{listerExists: false, clientExists: false},
		}
		_, _, err := observer.ObserveMemberObject(context.Background(), testClusterA, testSourceGVK, targetName)
		if err == nil {
			t.Fatal("expected error when no cluster client exists")
		}
	})

	t.Run("missing FTC is an error", func(t *testing.T) {
		dynamicClient := dynamicfake.NewSimpleDynamicClient(scheme.Scheme)
		observer := &KubeMemberObjectObserver{
			ftcManager: &fakeFTCManager{},
			fedInformerManager: &fakeFedInformerManager{
				listerExists: false, clientExists: true, dynamicClient: dynamicClient,
			},
		}
		_, _, err := observer.ObserveMemberObject(context.Background(), testClusterA, testSourceGVK, targetName)
		if err == nil {
			t.Fatal("expected error when no FTC exists")
		}
	})

	t.Run("direct GET NotFound is absent", func(t *testing.T) {
		dynamicClient := dynamicfake.NewSimpleDynamicClient(scheme.Scheme)
		observer := &KubeMemberObjectObserver{
			ftcManager: &fakeFTCManager{ftc: testFTC()},
			fedInformerManager: &fakeFedInformerManager{
				listerExists: false, clientExists: true, dynamicClient: dynamicClient,
			},
		}
		obj, found, err := observer.ObserveMemberObject(context.Background(), testClusterA, testSourceGVK, targetName)
		if err != nil || found || obj != nil {
			t.Fatalf("expected absent, got found=%v obj=%v err=%v", found, obj, err)
		}
	})
}

// TestWakerUsesWatchOnlyForWakeup verifies that informer events wake a waiter
// while the periodic re-query is disabled, proving the watch does not carry
// truth itself (the subsequent evaluation still reads persistent state).
func TestWakerUsesWatchOnlyForWakeup(t *testing.T) {
	limits := testLimits(time.Hour) // effectively disable periodic re-query
	fixture := newManagerFixture(t, limits)
	opaque, _ := fixture.issue(t, fixture.clusterA)
	_, outcomes := waitAsync(t, fixture.manager, opaque)
	assertBlocked(t, outcomes)

	// Persistent state becomes complete, but without an event nothing happens.
	fixture.markComplete(t, fixture.clusterA)
	assertBlocked(t, outcomes)

	handler := objectWakeHandler(fixture.manager, klog.Background())
	handler.OnAdd(fixture.fedObject, false)

	assertEventually(t, func() bool {
		select {
		case outcome := <-outcomes:
			return outcome.State == WaitCompleted
		default:
			return false
		}
	}, "object wake event did not trigger a re-evaluation")
}

func TestWakerHandlesTombstonesAndClusterEvents(t *testing.T) {
	limits := testLimits(time.Hour)
	fixture := newManagerFixture(t, limits)
	opaque, _ := fixture.issue(t, fixture.clusterA)
	_, outcomes := waitAsync(t, fixture.manager, opaque)

	// A delete tombstone must still produce a wake (it must not panic).
	objectHandler := objectWakeHandler(fixture.manager, klog.Background())
	objectHandler.OnDelete(cache.DeletedFinalStateUnknown{Obj: fixture.fedObject})

	// Cluster events wake every token.
	fixture.markComplete(t, fixture.clusterA)
	clusterHandler := clusterWakeHandler(fixture.manager)
	clusterHandler.OnUpdate(fixture.clusterA, fixture.clusterA)

	assertEventually(t, func() bool {
		select {
		case outcome := <-outcomes:
			return outcome.State == WaitCompleted
		default:
			return false
		}
	}, "cluster wake event did not trigger a re-evaluation")
}

// TestWakerIgnoresUnreadableObject ensures a malformed event neither panics
// nor wakes unrelated tokens.
func TestWakerIgnoresUnreadableObject(t *testing.T) {
	limits := testLimits(time.Hour)
	fixture := newManagerFixture(t, limits)
	opaque, _ := fixture.issue(t, fixture.clusterA)
	_, outcomes := waitAsync(t, fixture.manager, opaque)

	handler := objectWakeHandler(fixture.manager, klog.Background())
	handler.OnAdd(struct{}{}, false) // not a metav1 object

	assertBlocked(t, outcomes)

	// A wake for a different object must not complete this token either.
	fixture.markComplete(t, fixture.clusterA)
	other := &metav1.ObjectMeta{Namespace: testNamespace, Name: "different-object"}
	handler.OnUpdate(other, other)
	assertBlocked(t, outcomes)

	// The matching event still completes it.
	handler.OnUpdate(fixture.fedObject, fixture.fedObject)
	assertEventually(t, func() bool {
		select {
		case outcome := <-outcomes:
			return outcome.State == WaitCompleted
		default:
			return false
		}
	}, "matching wake event did not trigger completion")
}
