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

package scheduler

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/pointer"

	fedcorev1a1 "github.com/kubewharf/kubeadmiral/pkg/apis/core/v1alpha1"
	"github.com/kubewharf/kubeadmiral/pkg/controllers/scheduler/core"
	"github.com/kubewharf/kubeadmiral/pkg/controllers/scheduler/framework"
	schedulerledger "github.com/kubewharf/kubeadmiral/pkg/controllers/scheduler/ledger"
)

func newLedgerTestScheduler(t *testing.T) *Scheduler {
	t.Helper()
	cfg := framework.DefaultLedgerConfig()
	cfg.Enabled = true
	cfg.ReserveTimeout = 100 * time.Millisecond
	return &Scheduler{
		ledgerConfig: cfg,
		ledger:       schedulerledger.NewPendingPlacementLedger(cfg),
		attempts:     make(map[string]attemptState),
		knownUIDs:    make(map[string]types.UID),
	}
}

func testCluster(name string, uid types.UID, rv string, milliCPU int64) *fedcorev1a1.FederatedCluster {
	return &fedcorev1a1.FederatedCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, UID: uid, ResourceVersion: rv},
		Status: fedcorev1a1.FederatedClusterStatus{
			Resources: fedcorev1a1.Resources{
				Allocatable: corev1.ResourceList{
					corev1.ResourceCPU: *resource.NewMilliQuantity(milliCPU, resource.DecimalSI),
				},
				Available: corev1.ResourceList{
					corev1.ResourceCPU: *resource.NewMilliQuantity(milliCPU, resource.DecimalSI),
				},
			},
		},
	}
}

// TestAttemptBookkeeping verifies per-object attempt monotonicity and independent attempt sequences
// across federated objects.
func TestAttemptBookkeeping(t *testing.T) {
	s := newLedgerTestScheduler(t)

	first := s.nextAttempt("default/a", types.UID("uid-a"), 1)
	second := s.nextAttempt("default/a", types.UID("uid-a"), 1)
	if first != 1 || second != 2 {
		t.Fatalf("expected monotonically increasing attempts 1,2 got %d,%d", first, second)
	}
	other := s.nextAttempt("default/b", types.UID("uid-b"), 1)
	if other != 1 {
		t.Fatalf("expected independent attempt sequence for the second object, got %d", other)
	}

	s.trackFederatedUID("default/a", types.UID("uid-a"))
	uid, ok := s.forgetFederatedUID("default/a")
	if !ok || uid != types.UID("uid-a") {
		t.Fatalf("expected to retrieve tracked uid %s, got %s ok=%v", "uid-a", uid, ok)
	}
}

// TestBuildReservationPlanScaling verifies Divide mode scales the exact per-replica vector while
// Duplicate mode reserves the full vector per cluster, and that cluster identity/RV is propagated.
func TestBuildReservationPlanScaling(t *testing.T) {
	s := newLedgerTestScheduler(t)
	clusters := []*fedcorev1a1.FederatedCluster{
		testCluster("a", types.UID("uid-cluster-a"), "100", 10000),
		testCluster("b", types.UID("uid-cluster-b"), "200", 10000),
	}
	su := &framework.SchedulingUnit{
		FederatedUID:        types.UID("uid-fed"),
		FederatedGeneration: 3,
		ScheduleAttempt:     2,
		ResourceRequest:     framework.Resource{MilliCPU: 1000, Memory: 2048},
	}

	divideResult := core.ScheduleResult{SuggestedClusters: map[string]*int64{
		"a": pointer.Int64(3),
		"b": pointer.Int64(2),
	}}
	plan, err := s.buildReservationPlan(su, divideResult, clusters)
	if err != nil {
		t.Fatalf("buildReservationPlan failed: %v", err)
	}
	if plan.FederatedGeneration != 3 || plan.Attempt != 2 || len(plan.Items) != 2 {
		t.Fatalf("unexpected plan header: %+v", plan)
	}
	expected := map[string]framework.Resource{
		"a": {MilliCPU: 3000, Memory: 2048 * 3},
		"b": {MilliCPU: 2000, Memory: 2048 * 2},
	}
	for _, item := range plan.Items {
		want := expected[item.ClusterName]
		if item.Resources.MilliCPU != want.MilliCPU || item.Resources.Memory != want.Memory {
			t.Fatalf("unexpected scaled vector on %s: got %+v want %+v", item.ClusterName, item.Resources, want)
		}
	}

	duplicateResult := core.ScheduleResult{SuggestedClusters: map[string]*int64{
		"a": nil,
	}}
	duplicatePlan, err := s.buildReservationPlan(su, duplicateResult, clusters)
	if err != nil {
		t.Fatalf("duplicate buildReservationPlan failed: %v", err)
	}
	if duplicatePlan.Items[0].Resources.MilliCPU != 1000 {
		t.Fatalf("duplicate mode must reserve the full vector, got %d", duplicatePlan.Items[0].Resources.MilliCPU)
	}
}

// TestSchedulerLedgerEndToEndNoOverAllocation exercises the ledger wired into a scheduler: two workloads
// contend for one cluster, the second admission is rejected until the first is confirmed, and the
// accounts are fully reclaimed afterwards.
func TestSchedulerLedgerEndToEndNoOverAllocation(t *testing.T) {
	s := newLedgerTestScheduler(t)
	cluster := testCluster("a", types.UID("uid-cluster-a"), "100", 10000)
	// Open the epoch for admission and record the initial capacity basis.
	if err := s.ledger.Rebuild(context.Background(), framework.ReconcileInput{
		Clusters: []framework.ClusterObservation{{
			ClusterName:     cluster.Name,
			ClusterUID:      cluster.UID,
			ResourceVersion: cluster.ResourceVersion,
			Allocatable:     cluster.Status.Resources.Allocatable,
			Available:       cluster.Status.Resources.Available,
		}},
	}); err != nil {
		t.Fatalf("rebuild failed: %v", err)
	}

	su1 := &framework.SchedulingUnit{
		FederatedUID: types.UID("uid-fed-1"), FederatedGeneration: 1, ScheduleAttempt: 1,
		ResourceRequest: framework.Resource{MilliCPU: 3000},
	}
	plan1, err := s.buildReservationPlan(su1, core.ScheduleResult{SuggestedClusters: map[string]*int64{"a": nil}}, []*fedcorev1a1.FederatedCluster{cluster})
	if err != nil {
		t.Fatalf("plan1 failed: %v", err)
	}
	if err := s.ledger.Reserve(context.Background(), plan1); err != nil {
		t.Fatalf("reserve 1 failed: %v", err)
	}

	su2 := &framework.SchedulingUnit{
		FederatedUID: types.UID("uid-fed-2"), FederatedGeneration: 1, ScheduleAttempt: 1,
		ResourceRequest: framework.Resource{MilliCPU: 8000},
	}
	plan2, err := s.buildReservationPlan(su2, core.ScheduleResult{SuggestedClusters: map[string]*int64{"a": nil}}, []*fedcorev1a1.FederatedCluster{cluster})
	if err != nil {
		t.Fatalf("plan2 failed: %v", err)
	}
	if err := s.ledger.Reserve(context.Background(), plan2); err == nil {
		t.Fatalf("expected second workload to be rejected without over-allocation")
	}
	if reserved := s.ledger.ReservedResources(cluster.UID, nil); reserved.MilliCPU != 3000 {
		t.Fatalf("expected reserved vector to stay at 3000, got %d", reserved.MilliCPU)
	}

	// Confirm with evidence: advanced RV and OK propagation status. The refreshed member snapshot now
	// reports 7000 available, i.e. the 3000 demand is member-observed and must not be double charged.
	s.ledger.Reconcile(framework.ReconcileInput{
		Clusters: []framework.ClusterObservation{{
			ClusterName: cluster.Name, ClusterUID: cluster.UID, ResourceVersion: "101",
			Allocatable: cluster.Status.Resources.Allocatable,
			Available: corev1.ResourceList{
				corev1.ResourceCPU: *resource.NewMilliQuantity(7000, resource.DecimalSI),
			},
		}},
		Objects: []framework.ObjectObservation{{
			UID: types.UID("uid-fed-1"), Generation: 1,
			Clusters: []framework.ObjectClusterObservation{{
				ClusterName: "a", PropagationStatus: fedcorev1a1.ClusterPropagationOK, LastObservedGeneration: 1,
			}},
		}},
	})
	if s.ledger.Stats().TotalAccounts != 0 {
		t.Fatalf("expected confirmed account reclaimed, got %d", s.ledger.Stats().TotalAccounts)
	}

	// A 6000 vector now fits: member-observed demand 3000 + 6000 = 9000 <= 10000, proving the confirmed
	// reservation was released instead of being charged twice.
	su2.ResourceRequest = framework.Resource{MilliCPU: 6000}
	plan2, err = s.buildReservationPlan(su2, core.ScheduleResult{SuggestedClusters: map[string]*int64{"a": nil}}, []*fedcorev1a1.FederatedCluster{cluster})
	if err != nil {
		t.Fatalf("plan2 rebuild failed: %v", err)
	}
	if err := s.ledger.Reserve(context.Background(), plan2); err != nil {
		t.Fatalf("expected second workload admitted after reclaim: %v", err)
	}
	if reserved := s.ledger.ReservedResources(cluster.UID, nil); reserved.MilliCPU != 6000 {
		t.Fatalf("expected 6000 reserved after re-admission, got %d", reserved.MilliCPU)
	}
}
