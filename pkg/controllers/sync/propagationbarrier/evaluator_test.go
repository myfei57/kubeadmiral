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
	"errors"
	"reflect"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"

	fedcorev1a1 "github.com/kubewharf/kubeadmiral/pkg/apis/core/v1alpha1"
)

var errMemberRead = errors.New("simulated member read failure")

// scenario bundles the happy-path inputs; individual tests mutate copies.
type scenario struct {
	barrier        *PropagationBarrier
	fedObject      *fedcorev1a1.FederatedObject
	templateDigest string
	overrideDigest string
	pvStatus       *fedcorev1a1.PropagatedVersionStatus
	cluster        *fedcorev1a1.FederatedCluster
	memberObj      *unstructured.Unstructured
	memberFound    bool
	memberErr      error
}

func buildHappyScenario(t *testing.T) scenario {
	t.Helper()
	clock := newFakeClock()
	reader := newFakeStateReader()
	observer := newFakeMemberObjectObserver()
	manager := newTestManager(reader, observer, clock, testLimits(time.Second))

	template := newTemplate(2)
	fedObject := newFedObject(t, testFedUID, 3, template, nil, testClusterA)
	cluster := newCluster(testClusterA, testUIDA)
	memberObj := withSourceGeneration(newMemberObject(2, "rv-2", true), 2)

	opaque, barrier := issueTokenFor(t, manager, fedObject, targetFromCluster(cluster))
	if opaque == "" {
		t.Fatal("expected non-empty opaque token")
	}

	pvStatus := newPVStatus(barrier.TemplateDigest, barrier.OverrideDigest,
		map[string]string{testClusterA: "gen:2"})

	return scenario{
		barrier:        barrier,
		fedObject:      fedObject,
		templateDigest: barrier.TemplateDigest,
		overrideDigest: barrier.OverrideDigest,
		pvStatus:       pvStatus,
		cluster:        cluster,
		memberObj:      memberObj,
		memberFound:    true,
	}
}

func (s scenario) snapshot() *observationSnapshot {
	observation := &targetObservation{
		cluster:     s.cluster,
		memberObj:   s.memberObj,
		memberFound: s.memberFound,
		memberErr:   s.memberErr,
	}
	var fedObject fedcorev1a1.GenericFederatedObject
	if s.fedObject != nil {
		fedObject = s.fedObject
	}
	return &observationSnapshot{
		fedObject: fedObject,
		pvStatus:  s.pvStatus,
		targets:   map[string]*targetObservation{testClusterA: observation},
	}
}

func (s scenario) evaluate(t *testing.T) TargetResult {
	t.Helper()
	results := evaluate(s.barrier, s.snapshot(), newFakeClock().Now())
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	return results[0]
}

func TestEvaluateHappyPathCompletes(t *testing.T) {
	s := buildHappyScenario(t)
	result := s.evaluate(t)
	assertState(t, result, TargetComplete)

	// Results must preserve token target order.
	twoTarget := s
	clusterB := newCluster(testClusterB, testUIDB)
	twoTarget.barrier.Targets = append(twoTarget.barrier.Targets, targetFromCluster(clusterB))
	snap := twoTarget.snapshot()
	snap.targets[testClusterB] = &targetObservation{
		cluster:     clusterB,
		memberObj:   withSourceGeneration(newMemberObject(2, "rv-2-b", true), 2),
		memberFound: true,
	}
	twoTarget.pvStatus.ClusterVersions = append(twoTarget.pvStatus.ClusterVersions,
		fedcorev1a1.ClusterObjectVersion{ClusterName: testClusterB, Version: "gen:2"})
	snap.pvStatus = twoTarget.pvStatus

	results := evaluate(twoTarget.barrier, snap, newFakeClock().Now())
	if len(results) != 2 || results[0].ClusterName != testClusterA || results[1].ClusterName != testClusterB {
		t.Fatalf("results must follow token target order, got %+v", results)
	}
}

func TestEvaluateResourceVersionBasedIdentity(t *testing.T) {
	// Resources without a generation use an rv: receipt. The rv is only ever
	// compared within the same cluster, never ordered across clusters.
	s := buildHappyScenario(t)
	rvMember := newMemberObject(0, "rv-only-1", true)
	s.memberObj = rvMember
	s.pvStatus = newPVStatus(s.templateDigest, s.overrideDigest,
		map[string]string{testClusterA: "rv:rv-only-1"})
	assertState(t, s.evaluate(t), TargetComplete)

	s.memberObj = newMemberObject(0, "rv-only-2", true)
	result := s.evaluate(t)
	assertState(t, result, TargetUnobserved)
	if result.Reason != ReasonMemberVersionMismatch {
		t.Fatalf("expected %s, got %s", ReasonMemberVersionMismatch, result.Reason)
	}
}

func TestEvaluateStaleConditions(t *testing.T) {
	testCases := map[string]func(s scenario) (scenario, string){
		"federated object missing": func(s scenario) (scenario, string) {
			s.fedObject = nil
			return s, ReasonFedObjectMissing
		},
		"same-name object replacement changes UID": func(s scenario) (scenario, string) {
			replaced := newFedObject(t, types.UID("uid-fed-object-2"), 3, newTemplate(2), nil, testClusterA)
			s.fedObject = replaced
			return s, ReasonFedObjectUIDMismatch
		},
		"generation advanced": func(s scenario) (scenario, string) {
			s.fedObject.Generation = 4
			return s, ReasonGenerationMismatch
		},
		"template digest changed": func(s scenario) (scenario, string) {
			s.fedObject = newFedObject(t, testFedUID, 3, newTemplate(9), nil, testClusterA)
			return s, ReasonTemplateDigestMismatch
		},
		"override digest changed": func(s scenario) (scenario, string) {
			overrides := []fedcorev1a1.OverrideWithController{{Controller: "test"}}
			s.fedObject = newFedObject(t, testFedUID, 3, newTemplate(2), overrides, testClusterA)
			return s, ReasonOverrideDigestMismatch
		},
		"cluster removed": func(s scenario) (scenario, string) {
			s.cluster = nil
			return s, ReasonClusterMissing
		},
		"cluster rebuilt with same name new UID": func(s scenario) (scenario, string) {
			s.cluster = newCluster(testClusterA, types.UID("uid-cluster-a-rebuilt"))
			return s, ReasonClusterUIDMismatch
		},
		"placement no longer contains target": func(s scenario) (scenario, string) {
			s.fedObject = newFedObject(t, testFedUID, 3, newTemplate(2), nil, testClusterB)
			return s, ReasonPlacementChanged
		},
	}

	for name, mutate := range testCases {
		t.Run(name, func(t *testing.T) {
			s := buildHappyScenario(t)
			s, expectedReason := mutate(s)
			result := s.evaluate(t)
			assertState(t, result, TargetStale)
			if result.Reason != expectedReason {
				t.Fatalf("expected reason %s, got %s", expectedReason, result.Reason)
			}
		})
	}
}

func TestEvaluateMissingReceiptConditions(t *testing.T) {
	t.Run("no propagated version at all", func(t *testing.T) {
		s := buildHappyScenario(t)
		s.pvStatus = nil
		result := s.evaluate(t)
		assertState(t, result, TargetMissing)
		if result.Reason != ReasonNoDispatchReceipt {
			t.Fatalf("expected %s, got %s", ReasonNoDispatchReceipt, result.Reason)
		}
	})

	t.Run("old generation receipt has stale digests", func(t *testing.T) {
		s := buildHappyScenario(t)
		// The persisted receipt still describes the previous input identity.
		s.pvStatus = newPVStatus("old-template-digest", "old-override-digest",
			map[string]string{testClusterA: "gen:1"})
		result := s.evaluate(t)
		assertState(t, result, TargetMissing)
		if result.Reason != ReasonNoDispatchReceipt {
			t.Fatalf("expected %s, got %s", ReasonNoDispatchReceipt, result.Reason)
		}
	})

	t.Run("matching digests but no entry for target", func(t *testing.T) {
		s := buildHappyScenario(t)
		s.pvStatus = newPVStatus(s.templateDigest, s.overrideDigest,
			map[string]string{testClusterB: "gen:2"})
		result := s.evaluate(t)
		assertState(t, result, TargetMissing)
		if result.Reason != ReasonNoClusterReceipt {
			t.Fatalf("expected %s, got %s", ReasonNoClusterReceipt, result.Reason)
		}
	})
}

func TestEvaluateRecordedPropagationStatus(t *testing.T) {
	t.Run("explicit propagation error", func(t *testing.T) {
		s := buildHappyScenario(t)
		s.fedObject = withClusterStatus(s.fedObject, testClusterA, fedcorev1a1.UpdateFailed, 1)
		result := s.evaluate(t)
		assertState(t, result, TargetError)
		if result.Reason != ReasonPropagationError {
			t.Fatalf("expected %s, got %s", ReasonPropagationError, result.Reason)
		}
	})

	t.Run("pending create is missing not error", func(t *testing.T) {
		s := buildHappyScenario(t)
		s.fedObject = withClusterStatus(s.fedObject, testClusterA, fedcorev1a1.PendingCreate, 0)
		result := s.evaluate(t)
		assertState(t, result, TargetMissing)
		if result.Reason != ReasonPendingCreate {
			t.Fatalf("expected %s, got %s", ReasonPendingCreate, result.Reason)
		}
	})

	t.Run("waiting for removal is unobserved", func(t *testing.T) {
		s := buildHappyScenario(t)
		s.fedObject = withClusterStatus(s.fedObject, testClusterA, fedcorev1a1.WaitingForRemoval, 2)
		result := s.evaluate(t)
		assertState(t, result, TargetUnobserved)
		if result.Reason != ReasonRemovalPending {
			t.Fatalf("expected %s, got %s", ReasonRemovalPending, result.Reason)
		}
	})

	t.Run("OK status still requires member observation", func(t *testing.T) {
		s := buildHappyScenario(t)
		s.fedObject = withClusterStatus(s.fedObject, testClusterA, fedcorev1a1.ClusterPropagationOK, 2)
		s.memberFound = false
		s.memberObj = nil
		result := s.evaluate(t)
		assertState(t, result, TargetUnobserved)
	})
}

func TestEvaluateMemberObservation(t *testing.T) {
	t.Run("dispatch receipt exists but informer has not observed object", func(t *testing.T) {
		s := buildHappyScenario(t)
		s.memberFound = false
		s.memberObj = nil
		result := s.evaluate(t)
		assertState(t, result, TargetUnobserved)
		if result.Reason != ReasonMemberObjectNotObserved {
			t.Fatalf("expected %s, got %s", ReasonMemberObjectNotObserved, result.Reason)
		}
	})

	t.Run("member object lost managed label", func(t *testing.T) {
		s := buildHappyScenario(t)
		s.memberObj = newMemberObject(2, "rv-2", false)
		result := s.evaluate(t)
		assertState(t, result, TargetError)
		if result.Reason != ReasonMemberObjectNotManaged {
			t.Fatalf("expected %s, got %s", ReasonMemberObjectNotManaged, result.Reason)
		}
	})

	t.Run("member still runs old generation", func(t *testing.T) {
		s := buildHappyScenario(t)
		s.memberObj = withSourceGeneration(newMemberObject(1, "rv-1", true), 1)
		result := s.evaluate(t)
		assertState(t, result, TargetUnobserved)
		if result.Reason != ReasonMemberVersionMismatch {
			t.Fatalf("expected %s, got %s", ReasonMemberVersionMismatch, result.Reason)
		}
	})

	t.Run("stale source generation annotation", func(t *testing.T) {
		s := buildHappyScenario(t)
		// Version matches but the propagated annotation lags behind.
		s.memberObj = withSourceGeneration(newMemberObject(2, "rv-2", true), 1)
		result := s.evaluate(t)
		assertState(t, result, TargetUnobserved)
		if result.Reason != ReasonMemberGenerationMismatch {
			t.Fatalf("expected %s, got %s", ReasonMemberGenerationMismatch, result.Reason)
		}
	})

	t.Run("member retrieval error", func(t *testing.T) {
		s := buildHappyScenario(t)
		s.memberErr = errMemberRead
		result := s.evaluate(t)
		assertState(t, result, TargetError)
		if result.Reason != ReasonMemberRetrievalFailed {
			t.Fatalf("expected %s, got %s", ReasonMemberRetrievalFailed, result.Reason)
		}
	})

	t.Run("cluster read error", func(t *testing.T) {
		s := buildHappyScenario(t)
		s.cluster = nil
		snap := s.snapshot()
		snap.targets[testClusterA].memberErr = errMemberRead
		results := evaluate(s.barrier, snap, newFakeClock().Now())
		assertState(t, results[0], TargetError)
		if results[0].Reason != ReasonMemberRetrievalFailed {
			t.Fatalf("expected %s, got %s", ReasonMemberRetrievalFailed, results[0].Reason)
		}
	})
}

func TestEvaluateExpiredTokenIsStale(t *testing.T) {
	s := buildHappyScenario(t)
	expiredAt := newFakeClock()
	expiredAt.Advance(time.Hour)
	results := evaluate(s.barrier, s.snapshot(), expiredAt.Now())
	assertState(t, results[0], TargetStale)
	if results[0].Reason != ReasonTokenExpired {
		t.Fatalf("expected %s, got %s", ReasonTokenExpired, results[0].Reason)
	}
}

func TestEvaluateDoesNotMutateInputs(t *testing.T) {
	s := buildHappyScenario(t)
	s.fedObject = withClusterStatus(s.fedObject, testClusterA, fedcorev1a1.ClusterPropagationOK, 2)
	snap := s.snapshot()

	fedObjectBefore := s.fedObject.DeepCopy()
	pvBefore := s.pvStatus.DeepCopy()
	clusterBefore := s.cluster.DeepCopy()
	memberBefore := s.memberObj.DeepCopy()

	_ = evaluate(s.barrier, snap, newFakeClock().Now())

	if !reflect.DeepEqual(fedObjectBefore, s.fedObject) {
		t.Fatal("evaluator must not mutate the federated object")
	}
	if !reflect.DeepEqual(pvBefore, s.pvStatus) {
		t.Fatal("evaluator must not mutate the propagated version status")
	}
	if !reflect.DeepEqual(clusterBefore, s.cluster) {
		t.Fatal("evaluator must not mutate the cluster object")
	}
	if !reflect.DeepEqual(memberBefore, s.memberObj) {
		t.Fatal("evaluator must not mutate the member object")
	}
}

func TestEvaluateCollectedStatusIsNeverUsed(t *testing.T) {
	// Even if a stale CollectedStatus-like condition claimed success, the
	// evaluator only consults the pinned receipt and member observation.
	s := buildHappyScenario(t)
	s.pvStatus = nil
	s.fedObject.Status.Conditions = []fedcorev1a1.GenericFederatedObjectCondition{{
		Type:   fedcorev1a1.PropagationConditionType,
		Status: "True",
	}}
	result := s.evaluate(t)
	assertState(t, result, TargetMissing)
}
