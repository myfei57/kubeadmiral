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

package ledger

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	fedcorev1a1 "github.com/kubewharf/kubeadmiral/pkg/apis/core/v1alpha1"
	"github.com/kubewharf/kubeadmiral/pkg/controllers/scheduler/framework"
)

const (
	clusterAUID types.UID = "cluster-a-uid"
	clusterBUID types.UID = "cluster-b-uid"
	fedAUID     types.UID = "fed-a-uid"
	fedBUID     types.UID = "fed-b-uid"
)

func testConfig() framework.LedgerConfig {
	cfg := framework.DefaultLedgerConfig()
	cfg.Enabled = true
	cfg.ReserveTimeout = 200 * time.Millisecond
	cfg.TTL = time.Second
	cfg.MaxAccounts = 16
	cfg.MaxAccountsPerCluster = 8
	cfg.MaxAccountsPerFederatedObject = 8
	cfg.MaxWaiters = 4
	return cfg
}

func clusterObservation(name string, uid types.UID, rv string, allocatable, available corev1.ResourceList) framework.ClusterObservation {
	return framework.ClusterObservation{
		ClusterName:     name,
		ClusterUID:      uid,
		ResourceVersion: rv,
		Allocatable:     allocatable,
		Available:       available,
	}
}

func cpuList(milliCPU int64) corev1.ResourceList {
	return corev1.ResourceList{
		corev1.ResourceCPU: *resource.NewMilliQuantity(milliCPU, resource.DecimalSI),
	}
}

func cpuResource(milliCPU int64) framework.Resource {
	return framework.Resource{MilliCPU: milliCPU}
}

func plan(uid types.UID, generation, attempt int64, items ...framework.ReservationPlanItem) framework.ReservationPlan {
	return framework.ReservationPlan{
		FederatedUID:        uid,
		FederatedGeneration: generation,
		Attempt:             attempt,
		Items:               items,
	}
}

func item(name string, uid types.UID, rv string, r framework.Resource) framework.ReservationPlanItem {
	return framework.ReservationPlanItem{
		ClusterName:                  name,
		ClusterUID:                   uid,
		Resources:                    r,
		ClusterStatusResourceVersion: rv,
	}
}

func seedSnapshots(t *testing.T, l framework.PendingPlacementLedger, clusters ...framework.ClusterObservation) {
	t.Helper()
	// Completing the bounded rebuild opens admission and records the initial capacity basis.
	if err := l.Rebuild(context.Background(), framework.ReconcileInput{Clusters: clusters}); err != nil {
		t.Fatalf("seed rebuild failed: %v", err)
	}
}

func key(uid types.UID, generation, attempt int64, clusterUID types.UID) framework.LedgerKey {
	return framework.LedgerKey{
		FederatedUID:        uid,
		FederatedGeneration: generation,
		ClusterUID:          clusterUID,
		Attempt:             attempt,
	}
}

func objectClusterStatus(
	name string,
	status fedcorev1a1.PropagationStatusType,
	observedGeneration int64,
) []framework.ObjectClusterObservation {
	return []framework.ObjectClusterObservation{{
		ClusterName:            name,
		PropagationStatus:      status,
		LastObservedGeneration: observedGeneration,
	}}
}

// TestReserveAllOrNothingPartialClusterFailure verifies that a multi-cluster plan is committed atomically:
// when one leg cannot be admitted, no account is created on any cluster.
func TestReserveAllOrNothingPartialClusterFailure(t *testing.T) {
	l := NewPendingPlacementLedger(testConfig())
	seedSnapshots(t, l,
		clusterObservation("a", clusterAUID, "100", cpuList(10000), cpuList(10000)),
		clusterObservation("b", clusterBUID, "200", cpuList(1000), cpuList(1000)),
	)

	badPlan := plan(fedAUID, 1, 1,
		item("a", clusterAUID, "100", cpuResource(6000)),
		item("b", clusterBUID, "200", cpuResource(6000)),
	)
	err := l.Reserve(context.Background(), badPlan)
	if !errors.Is(err, framework.ErrInsufficientReservedCapacity) {
		t.Fatalf("expected ErrInsufficientReservedCapacity, got %v", err)
	}
	if stats := l.Stats(); stats.TotalAccounts != 0 {
		t.Fatalf("expected no accounts after failed multi-cluster reserve, got %d", stats.TotalAccounts)
	}

	goodPlan := plan(fedAUID, 1, 1,
		item("a", clusterAUID, "100", cpuResource(6000)),
		item("b", clusterBUID, "200", cpuResource(500)),
	)
	if err := l.Reserve(context.Background(), goodPlan); err != nil {
		t.Fatalf("expected atomic reserve to succeed, got %v", err)
	}
	if stats := l.Stats(); stats.TotalAccounts != 2 {
		t.Fatalf("expected 2 accounts, got %d", stats.TotalAccounts)
	}
}

// TestConcurrentWorkloadsNoOverAllocation drives two workloads against one tight cluster and asserts the
// admission account never over-commits capacity; capacity becomes reusable only after the first workload
// is confirmed and released.
func TestConcurrentWorkloadsNoOverAllocation(t *testing.T) {
	cfg := testConfig()
	cfg.ReserveTimeout = 100 * time.Millisecond
	l := NewPendingPlacementLedger(cfg)
	seedSnapshots(t, l, clusterObservation("a", clusterAUID, "100", cpuList(10000), cpuList(10000)))

	first := plan(fedAUID, 1, 1, item("a", clusterAUID, "100", cpuResource(6000)))
	if err := l.Reserve(context.Background(), first); err != nil {
		t.Fatalf("first workload reserve failed: %v", err)
	}

	second := plan(fedBUID, 1, 1, item("a", clusterAUID, "100", cpuResource(6000)))

	// Concurrent attempts from multiple goroutines must all fail while capacity is insufficient; at no
	// point may the reserved vector exceed the allocatable capacity.
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := l.Reserve(context.Background(), second); err == nil {
				t.Errorf("over-allocation: second workload was admitted beyond capacity")
			}
		}()
	}
	wg.Wait()

	reserved := l.ReservedResources(clusterAUID, nil)
	if reserved.MilliCPU != 6000 {
		t.Fatalf("expected exactly 6000 milli-cpu reserved, got %d", reserved.MilliCPU)
	}
	if l.Stats().TotalAccounts != 1 {
		t.Fatalf("expected only the first workload account, got %d", l.Stats().TotalAccounts)
	}

	// Confirmation reclaims the account and the second workload is then admissible.
	ok, err := l.Confirm(key(fedAUID, 1, 1, clusterAUID))
	if err != nil || !ok {
		t.Fatalf("expected confirmation to release the first workload, ok=%v err=%v", ok, err)
	}
	if err := l.Reserve(context.Background(), second); err != nil {
		t.Fatalf("second workload should be admitted after release, got %v", err)
	}
	reserved = l.ReservedResources(clusterAUID, nil)
	if reserved.MilliCPU != 6000 {
		t.Fatalf("expected 6000 milli-cpu reserved after re-admission, got %d", reserved.MilliCPU)
	}
}

// TestReserveReplacesSameKeyWithoutDoubleCharging ensures re-reserving the exact attempt replaces the
// vector instead of stacking it, and that the Fit-side exclusion of the caller's own key works.
func TestReserveReplacesSameKeyWithoutDoubleCharging(t *testing.T) {
	l := NewPendingPlacementLedger(testConfig())
	seedSnapshots(t, l, clusterObservation("a", clusterAUID, "100", cpuList(10000), cpuList(10000)))

	p := plan(fedAUID, 1, 1, item("a", clusterAUID, "100", cpuResource(4000)))
	if err := l.Reserve(context.Background(), p); err != nil {
		t.Fatalf("reserve failed: %v", err)
	}
	p.Items[0].Resources = cpuResource(7000)
	if err := l.Reserve(context.Background(), p); err != nil {
		t.Fatalf("same-key replace reserve failed: %v", err)
	}
	if l.Stats().TotalAccounts != 1 {
		t.Fatalf("expected one account after same-key replace, got %d", l.Stats().TotalAccounts)
	}
	reserved := l.ReservedResources(clusterAUID, nil)
	if reserved.MilliCPU != 7000 {
		t.Fatalf("expected replaced vector of 7000, got %d", reserved.MilliCPU)
	}
	own := key(fedAUID, 1, 1, clusterAUID)
	if got := l.ReservedResources(clusterAUID, &own); got.MilliCPU != 0 {
		t.Fatalf("expected own key exclusion to yield zero, got %d", got.MilliCPU)
	}
}

// TestGenerationUpdateOnlyReleasesOwningAttempt covers generation replacement: cancelling the old
// generation releases only its own attempt and can never touch the new generation's accounts.
func TestGenerationUpdateOnlyReleasesOwningAttempt(t *testing.T) {
	l := NewPendingPlacementLedger(testConfig())
	seedSnapshots(t, l,
		clusterObservation("a", clusterAUID, "100", cpuList(20000), cpuList(20000)),
	)

	gen1 := plan(fedAUID, 1, 1, item("a", clusterAUID, "100", cpuResource(4000)))
	gen2 := plan(fedAUID, 2, 1, item("a", clusterAUID, "100", cpuResource(4000)))
	if err := l.Reserve(context.Background(), gen1); err != nil {
		t.Fatalf("gen1 reserve failed: %v", err)
	}
	if err := l.Reserve(context.Background(), gen2); err != nil {
		t.Fatalf("gen2 reserve failed: %v", err)
	}

	released := l.ReleaseAttempt(key(fedAUID, 1, 1, clusterAUID))
	if released != 1 {
		t.Fatalf("expected to release exactly 1 old-generation account, got %d", released)
	}
	if l.Stats().TotalAccounts != 1 {
		t.Fatalf("expected new generation account to survive, got %d accounts", l.Stats().TotalAccounts)
	}

	// A late confirmation carrying the stale generation cannot release the new attempt.
	ok, err := l.Confirm(key(fedAUID, 1, 1, clusterAUID))
	if err != nil || ok {
		t.Fatalf("stale confirmation must be a no-op, ok=%v err=%v", ok, err)
	}
	ok, err = l.Confirm(key(fedAUID, 2, 1, clusterAUID))
	if err != nil || !ok {
		t.Fatalf("exact new-generation confirmation should release, ok=%v err=%v", ok, err)
	}
	if l.Stats().TotalAccounts != 0 {
		t.Fatalf("expected all accounts reclaimed, got %d", l.Stats().TotalAccounts)
	}
}

// TestOutOfOrderConfirmation verifies confirmations are matched by exact key: confirming a newer attempt
// first leaves the older attempt intact, and a wrong-attempt confirmation never releases anything.
func TestOutOfOrderConfirmation(t *testing.T) {
	l := NewPendingPlacementLedger(testConfig())
	seedSnapshots(t, l, clusterObservation("a", clusterAUID, "100", cpuList(20000), cpuList(20000)))

	if err := l.Reserve(context.Background(), plan(fedAUID, 1, 1, item("a", clusterAUID, "100", cpuResource(1000)))); err != nil {
		t.Fatalf("attempt 1 reserve failed: %v", err)
	}
	if err := l.Reserve(context.Background(), plan(fedAUID, 1, 2, item("a", clusterAUID, "100", cpuResource(1000)))); err != nil {
		t.Fatalf("attempt 2 reserve failed: %v", err)
	}

	// Confirm the newer attempt first (out of order).
	ok, err := l.Confirm(key(fedAUID, 1, 2, clusterAUID))
	if err != nil || !ok {
		t.Fatalf("attempt 2 confirmation failed, ok=%v err=%v", ok, err)
	}
	if _, exists := l.Stats().PerFederatedAccounts[string(fedAUID)]; !exists {
		t.Fatalf("federated index should still exist")
	}
	if l.Stats().TotalAccounts != 1 {
		t.Fatalf("expected attempt 1 account to remain, got %d accounts", l.Stats().TotalAccounts)
	}

	// A confirmation with a non-existent attempt is a no-op rather than releasing attempt 1.
	ok, err = l.Confirm(key(fedAUID, 1, 99, clusterAUID))
	if err != nil || ok {
		t.Fatalf("unknown attempt confirmation must be a no-op, ok=%v err=%v", ok, err)
	}
	if l.Stats().TotalAccounts != 1 {
		t.Fatalf("wrong-attempt confirmation released an account, got %d", l.Stats().TotalAccounts)
	}

	ok, err = l.Confirm(key(fedAUID, 1, 1, clusterAUID))
	if err != nil || !ok {
		t.Fatalf("attempt 1 confirmation failed, ok=%v err=%v", ok, err)
	}
	if l.Stats().TotalAccounts != 0 {
		t.Fatalf("expected full reclaim, got %d", l.Stats().TotalAccounts)
	}
}

// TestReconcileConfirmationBasis ensures member inclusion is only assumed on explicit evidence: OK
// propagation status at the reservation generation plus a cluster status RV strictly newer than the
// reservation basis. Timestamps are never trusted, and unprovable cases are retained conservatively.
func TestReconcileConfirmationBasis(t *testing.T) {
	l := NewPendingPlacementLedger(testConfig())
	seedSnapshots(t, l, clusterObservation("a", clusterAUID, "100", cpuList(10000), cpuList(10000)))

	if err := l.Reserve(context.Background(), plan(fedAUID, 1, 1, item("a", clusterAUID, "100", cpuResource(3000)))); err != nil {
		t.Fatalf("reserve failed: %v", err)
	}

	obj := framework.ObjectObservation{
		UID:        fedAUID,
		Generation: 1,
		Clusters: []framework.ObjectClusterObservation{{
			ClusterName:            "a",
			PropagationStatus:      fedcorev1a1.ClusterPropagationOK,
			LastObservedGeneration: 1,
		}},
	}

	// Same RV as the reservation basis: inclusion cannot be proven, retain.
	result := l.Reconcile(framework.ReconcileInput{
		Clusters: []framework.ClusterObservation{clusterObservation("a", clusterAUID, "100", cpuList(10000), cpuList(7000))},
		Objects:  []framework.ObjectObservation{obj},
	})
	if len(result.Retained) != 1 || l.Stats().TotalAccounts != 1 {
		t.Fatalf("expected conservative retention on unchanged RV, retained=%d accounts=%d", len(result.Retained), l.Stats().TotalAccounts)
	}

	// Non-OK status even with a newer RV: retain.
	result = l.Reconcile(framework.ReconcileInput{
		Clusters: []framework.ClusterObservation{clusterObservation("a", clusterAUID, "101", cpuList(10000), cpuList(7000))},
		Objects: []framework.ObjectObservation{{
			UID:        fedAUID,
			Generation: 1,
			Clusters:   objectClusterStatus("a", fedcorev1a1.PendingCreate, 1),
		}},
	})
	if len(result.Confirmed) != 0 || l.Stats().TotalAccounts != 1 {
		t.Fatalf("expected retention without OK status, confirmed=%d accounts=%d", len(result.Confirmed), l.Stats().TotalAccounts)
	}

	// Newer RV + OK + observed generation: inclusion provable, confirm and reclaim.
	result = l.Reconcile(framework.ReconcileInput{
		Clusters: []framework.ClusterObservation{clusterObservation("a", clusterAUID, "101", cpuList(10000), cpuList(7000))},
		Objects:  []framework.ObjectObservation{obj},
	})
	if len(result.Confirmed) != 1 || l.Stats().TotalAccounts != 0 {
		t.Fatalf("expected evidence-based confirmation, confirmed=%d accounts=%d", len(result.Confirmed), l.Stats().TotalAccounts)
	}
}

// TestReconcileReleasesSupersededGeneration verifies that observing a newer object generation releases
// only the old generation's accounts, while other objects remain untouched.
func TestReconcileReleasesSupersededGeneration(t *testing.T) {
	l := NewPendingPlacementLedger(testConfig())
	seedSnapshots(t, l, clusterObservation("a", clusterAUID, "100", cpuList(20000), cpuList(20000)))

	if err := l.Reserve(context.Background(), plan(fedAUID, 1, 1, item("a", clusterAUID, "100", cpuResource(1000)))); err != nil {
		t.Fatalf("fedA reserve failed: %v", err)
	}
	if err := l.Reserve(context.Background(), plan(fedBUID, 1, 1, item("a", clusterAUID, "100", cpuResource(1000)))); err != nil {
		t.Fatalf("fedB reserve failed: %v", err)
	}

	result := l.Reconcile(framework.ReconcileInput{
		Clusters: []framework.ClusterObservation{clusterObservation("a", clusterAUID, "100", cpuList(20000), cpuList(20000))},
		Objects: []framework.ObjectObservation{
			{UID: fedAUID, Generation: 2, Clusters: nil},
		},
	})
	if len(result.Released) != 1 {
		t.Fatalf("expected exactly one superseded account released, got %d", len(result.Released))
	}
	if l.Stats().TotalAccounts != 1 {
		t.Fatalf("expected fedB account to remain, got %d accounts", l.Stats().TotalAccounts)
	}
	if _, ok := l.Stats().PerFederatedAccounts[string(fedBUID)]; !ok {
		t.Fatalf("fedB accounts must not be touched")
	}
}

// TestLeaderChangeEpochAndRebuild models a leader restart: the new ledger mints a new epoch, fails closed
// while rebuilding, conservatively reconstructs unconfirmed placements under the new epoch and rejects
// stale-epoch commits.
func TestLeaderChangeEpochAndRebuild(t *testing.T) {
	cfg := testConfig()
	oldLeader := NewPendingPlacementLedger(cfg)
	seedSnapshots(t, oldLeader, clusterObservation("a", clusterAUID, "100", cpuList(10000), cpuList(10000)))
	if err := oldLeader.Reserve(context.Background(), plan(fedAUID, 1, 1, item("a", clusterAUID, "100", cpuResource(3000)))); err != nil {
		t.Fatalf("old leader reserve failed: %v", err)
	}

	newLeader := NewPendingPlacementLedger(cfg)
	if newLeader.Epoch() == oldLeader.Epoch() {
		t.Fatalf("leader restart must mint a new epoch")
	}

	// Before rebuild completes, admission fails closed.
	err := newLeader.Reserve(context.Background(), plan(fedBUID, 1, 1, item("a", clusterAUID, "100", cpuResource(1000))))
	if !errors.Is(err, framework.ErrLedgerRebuilding) {
		t.Fatalf("expected ErrLedgerRebuilding before rebuild, got %v", err)
	}

	input := framework.ReconcileInput{
		Clusters: []framework.ClusterObservation{clusterObservation("a", clusterAUID, "100", cpuList(10000), cpuList(10000))},
		Objects: []framework.ObjectObservation{{
			UID:        fedAUID,
			Generation: 1,
			Clusters:   objectClusterStatus("a", fedcorev1a1.PendingCreate, 1),
		}},
	}
	if err := newLeader.Rebuild(context.Background(), input); err != nil {
		t.Fatalf("rebuild failed: %v", err)
	}
	stats := newLeader.Stats()
	if stats.Rebuilding || stats.TotalAccounts != 1 {
		t.Fatalf("expected one conservatively rebuilt account, got %+v", stats)
	}
	oldAttemptKey := framework.LedgerKey{
		FederatedUID:        fedAUID,
		FederatedGeneration: 1,
		Attempt:             1,
		ClusterUID:          clusterAUID,
	}
	if ok, _ := newLeader.Confirm(oldAttemptKey); ok {
		t.Fatalf("old leader's attempt key must not exist in the new epoch")
	}
	if reserved := newLeader.ReservedResources(clusterAUID, nil); reserved.MilliCPU != 0 {
		t.Fatalf("rebuilt placeholder must be zero-vector until re-reserved, got %d", reserved.MilliCPU)
	}

	// After rebuild the new epoch admits fresh placements.
	if err := newLeader.Reserve(context.Background(), plan(fedBUID, 1, 1, item("a", clusterAUID, "100", cpuResource(2000)))); err != nil {
		t.Fatalf("new epoch admission failed: %v", err)
	}

	// The old leader loses leadership and can no longer commit anything.
	oldLeader.Deactivate()
	err = oldLeader.Reserve(context.Background(), plan(fedBUID, 1, 1, item("a", clusterAUID, "100", cpuResource(1000))))
	if !errors.Is(err, framework.ErrLedgerInactive) {
		t.Fatalf("expected ErrLedgerInactive on stale epoch, got %v", err)
	}
}

// TestRebuildReclaimsOnEvidence verifies rebuilt placeholder accounts are eventually reclaimed once the
// member evidence (OK status plus advanced RV) arrives.
func TestRebuildReclaimsOnEvidence(t *testing.T) {
	l := NewPendingPlacementLedger(testConfig())
	input := framework.ReconcileInput{
		Clusters: []framework.ClusterObservation{clusterObservation("a", clusterAUID, "100", cpuList(10000), cpuList(10000))},
		Objects: []framework.ObjectObservation{{
			UID:        fedAUID,
			Generation: 1,
			Clusters:   objectClusterStatus("a", fedcorev1a1.PendingCreate, 1),
		}},
	}
	if err := l.Rebuild(context.Background(), input); err != nil {
		t.Fatalf("rebuild failed: %v", err)
	}
	if l.Stats().TotalAccounts != 1 {
		t.Fatalf("expected rebuilt account, got %d", l.Stats().TotalAccounts)
	}

	confirmed := l.Reconcile(framework.ReconcileInput{
		Clusters: []framework.ClusterObservation{clusterObservation("a", clusterAUID, "101", cpuList(10000), cpuList(7000))},
		Objects: []framework.ObjectObservation{{
			UID:        fedAUID,
			Generation: 1,
			Clusters:   objectClusterStatus("a", fedcorev1a1.ClusterPropagationOK, 1),
		}},
	})
	if len(confirmed.Confirmed) != 1 || l.Stats().TotalAccounts != 0 {
		t.Fatalf("expected rebuilt account to be reclaimed, confirmed=%d accounts=%d",
			len(confirmed.Confirmed), l.Stats().TotalAccounts)
	}
}

// TestAccountCapsAndTTLStalePause checks the finite bounds: global/per-cluster caps reject new accounts,
// TTL flags stale accounts without releasing them without evidence, and the stale cap pauses admission.
func TestAccountCapsAndTTLStalePause(t *testing.T) {
	cfg := testConfig()
	cfg.MaxAccounts = 2
	cfg.MaxAccountsPerCluster = 2
	cfg.MaxAccountsPerFederatedObject = 2
	cfg.TTL = 10 * time.Millisecond
	l := NewPendingPlacementLedger(cfg)
	seedSnapshots(t, l,
		clusterObservation("a", clusterAUID, "100", cpuList(100000), cpuList(100000)),
		clusterObservation("b", clusterBUID, "200", cpuList(100000), cpuList(100000)),
	)

	if err := l.Reserve(context.Background(), plan(fedAUID, 1, 1, item("a", clusterAUID, "100", cpuResource(1000)))); err != nil {
		t.Fatalf("first reserve failed: %v", err)
	}
	if err := l.Reserve(context.Background(), plan(fedAUID, 1, 2, item("b", clusterBUID, "200", cpuResource(1000)))); err != nil {
		t.Fatalf("second reserve failed: %v", err)
	}
	err := l.Reserve(context.Background(), plan(fedBUID, 1, 1, item("a", clusterAUID, "100", cpuResource(1000))))
	if !errors.Is(err, framework.ErrLedgerAccountLimit) {
		t.Fatalf("expected ErrLedgerAccountLimit, got %v", err)
	}

	time.Sleep(20 * time.Millisecond)
	sweep := l.SweepExpired(time.Now(), framework.ReconcileInput{
		Clusters: []framework.ClusterObservation{
			clusterObservation("a", clusterAUID, "100", cpuList(100000), cpuList(100000)),
			clusterObservation("b", clusterBUID, "200", cpuList(100000), cpuList(100000)),
		},
	})
	if sweep.Stale != 2 {
		t.Fatalf("expected 2 stale accounts, got %+v", sweep)
	}
	if l.Stats().TotalAccounts != 2 || l.Stats().StaleAccounts != 2 {
		t.Fatalf("TTL must not release unprovable accounts, got %+v", l.Stats())
	}

	// Stale cap (MaxAccounts/10) is exceeded: admission pauses fail-closed.
	err = l.Reserve(context.Background(), plan(fedAUID, 1, 3, item("a", clusterAUID, "100", cpuResource(1000))))
	if !errors.Is(err, framework.ErrLedgerStaleLimit) {
		t.Fatalf("expected ErrLedgerStaleLimit, got %v", err)
	}
}

// TestWaiterCap verifies the fair-waiting queue is bounded.
func TestWaiterCap(t *testing.T) {
	cfg := testConfig()
	cfg.MaxWaiters = 1
	cfg.ReserveTimeout = 200 * time.Millisecond
	l := NewPendingPlacementLedger(cfg)
	seedSnapshots(t, l, clusterObservation("a", clusterAUID, "100", cpuList(10000), cpuList(10000)))
	if err := l.Reserve(context.Background(), plan(fedAUID, 1, 1, item("a", clusterAUID, "100", cpuResource(9000)))); err != nil {
		t.Fatalf("seed reserve failed: %v", err)
	}

	var wg sync.WaitGroup
	errs := make([]error, 3)
	waiterUIDs := []types.UID{"fed-waiter-1", "fed-waiter-2", "fed-waiter-3"}
	for i := range errs {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errs[idx] = l.Reserve(context.Background(), plan(waiterUIDs[idx], 1, 1, item("a", clusterAUID, "100", cpuResource(5000))))
		}(i)
	}
	wg.Wait()

	limit := 0
	for _, err := range errs {
		if errors.Is(err, framework.ErrTooManyWaiters) {
			limit++
		}
	}
	if limit == 0 {
		t.Fatalf("expected at least one ErrTooManyWaiters, got %v", errs)
	}
}

// TestReleaseFederatedObject verifies deletion reclaims every account of the object across generations
// and attempts, while other objects are untouched.
func TestReleaseFederatedObject(t *testing.T) {
	l := NewPendingPlacementLedger(testConfig())
	seedSnapshots(t, l, clusterObservation("a", clusterAUID, "100", cpuList(20000), cpuList(20000)))
	if err := l.Reserve(context.Background(), plan(fedAUID, 1, 1, item("a", clusterAUID, "100", cpuResource(1000)))); err != nil {
		t.Fatalf("reserve A1 failed: %v", err)
	}
	if err := l.Reserve(context.Background(), plan(fedAUID, 2, 1, item("a", clusterAUID, "100", cpuResource(1000)))); err != nil {
		t.Fatalf("reserve A2 failed: %v", err)
	}
	if err := l.Reserve(context.Background(), plan(fedBUID, 1, 1, item("a", clusterAUID, "100", cpuResource(1000)))); err != nil {
		t.Fatalf("reserve B failed: %v", err)
	}
	if released := l.ReleaseFederatedObject(fedAUID); released != 2 {
		t.Fatalf("expected 2 accounts released for fedA, got %d", released)
	}
	if l.Stats().TotalAccounts != 1 {
		t.Fatalf("expected fedB account to remain, got %d", l.Stats().TotalAccounts)
	}
}

// TestScaleResource checks exact integer scaling and overflow rejection.
func TestScaleResource(t *testing.T) {
	base := framework.Resource{
		MilliCPU:         100,
		Memory:           200,
		EphemeralStorage: 300,
		ScalarResources: map[corev1.ResourceName]int64{
			corev1.ResourceName("example.com/gpu"): 2,
		},
	}
	scaled, err := ScaleResource(base, 3)
	if err != nil {
		t.Fatalf("scale failed: %v", err)
	}
	if scaled.MilliCPU != 300 || scaled.Memory != 600 || scaled.EphemeralStorage != 900 ||
		scaled.ScalarResources["example.com/gpu"] != 6 {
		t.Fatalf("unexpected scaled resource: %+v", scaled)
	}
	if _, err := ScaleResource(framework.Resource{MilliCPU: 1 << 62}, 4); err == nil {
		t.Fatalf("expected overflow error")
	}
	zero, err := ScaleResource(base, 0)
	if err != nil || zero.MilliCPU != 0 {
		t.Fatalf("expected zero resource, got %+v err=%v", zero, err)
	}
}

// TestInvalidPlanRejected ensures malformed plans never create accounts.
func TestInvalidPlanRejected(t *testing.T) {
	l := NewPendingPlacementLedger(testConfig())
	seedSnapshots(t, l, clusterObservation("a", clusterAUID, "100", cpuList(10000), cpuList(10000)))
	if err := l.Reserve(context.Background(), framework.ReservationPlan{Attempt: 1}); !errors.Is(err, framework.ErrInvalidReservationPlan) {
		t.Fatalf("expected invalid plan error, got %v", err)
	}
	duplicate := plan(fedAUID, 1, 1,
		item("a", clusterAUID, "100", cpuResource(1000)),
		item("a", clusterAUID, "100", cpuResource(1000)),
	)
	if err := l.Reserve(context.Background(), duplicate); !errors.Is(err, framework.ErrInvalidReservationPlan) {
		t.Fatalf("expected duplicate cluster plan error, got %v", err)
	}
	if l.Stats().TotalAccounts != 0 {
		t.Fatalf("invalid plans must not create accounts, got %d", l.Stats().TotalAccounts)
	}
}

// TestUnknownClusterFailsClosed verifies admission pauses when no capacity snapshot basis exists.
func TestUnknownClusterFailsClosed(t *testing.T) {
	l := NewPendingPlacementLedger(testConfig())
	// Rebuild completes without any cluster basis.
	if err := l.Rebuild(context.Background(), framework.ReconcileInput{}); err != nil {
		t.Fatalf("rebuild failed: %v", err)
	}
	err := l.Reserve(context.Background(), plan(fedAUID, 1, 1, item("a", clusterAUID, "100", cpuResource(1000))))
	if !errors.Is(err, framework.ErrInsufficientReservedCapacity) {
		t.Fatalf("expected fail-closed capacity error for unknown cluster, got %v", err)
	}
}

// TestClusterRemovalReclaims verifies accounts on a vanished cluster are reclaimed during reconcile.
func TestClusterRemovalReclaims(t *testing.T) {
	l := NewPendingPlacementLedger(testConfig())
	seedSnapshots(t, l, clusterObservation("a", clusterAUID, "100", cpuList(10000), cpuList(10000)))
	if err := l.Reserve(context.Background(), plan(fedAUID, 1, 1, item("a", clusterAUID, "100", cpuResource(1000)))); err != nil {
		t.Fatalf("reserve failed: %v", err)
	}
	result := l.Reconcile(framework.ReconcileInput{
		Clusters: []framework.ClusterObservation{},
		Objects: []framework.ObjectObservation{{
			UID: fedAUID, Generation: 1,
			Clusters: []framework.ObjectClusterObservation{{ClusterName: "a", PropagationStatus: fedcorev1a1.PendingCreate}},
		}},
	})
	if len(result.Released) != 1 || l.Stats().TotalAccounts != 0 {
		t.Fatalf("expected cluster removal to release the account, released=%d accounts=%d",
			len(result.Released), l.Stats().TotalAccounts)
	}
}

// TestFederatedClusterTypeCompatibility is a compile-time guard that the observation types stay
// compatible with the v1alpha1 API.
func TestFederatedClusterTypeCompatibility(t *testing.T) {
	cluster := &fedcorev1a1.FederatedCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "a", UID: clusterAUID, ResourceVersion: "100"},
		Status: fedcorev1a1.FederatedClusterStatus{
			Resources: fedcorev1a1.Resources{
				Allocatable: cpuList(10000),
				Available:   cpuList(10000),
			},
		},
	}
	observation := framework.ClusterObservation{
		ClusterName:     cluster.Name,
		ClusterUID:      cluster.UID,
		ResourceVersion: cluster.ResourceVersion,
		Allocatable:     cluster.Status.Resources.Allocatable,
		Available:       cluster.Status.Resources.Available,
	}
	if observation.ClusterUID != clusterAUID {
		t.Fatalf("unexpected cluster UID: %s", observation.ClusterUID)
	}
}
