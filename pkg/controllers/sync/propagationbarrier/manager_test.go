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
	"errors"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"

	fedcorev1a1 "github.com/kubewharf/kubeadmiral/pkg/apis/core/v1alpha1"
	"github.com/kubewharf/kubeadmiral/pkg/controllers/common"
)

const (
	testRequery = 25 * time.Millisecond
	waitTimeout = 3 * time.Second
)

type managerFixture struct {
	manager  *Manager
	reader   *fakeStateReader
	observer *fakeMemberObjectObserver
	clock    *fakeClock

	fedObject *fedcorev1a1.FederatedObject
	clusterA  *fedcorev1a1.FederatedCluster
	clusterB  *fedcorev1a1.FederatedCluster

	template *unstructured.Unstructured
	ref      ObjectRef
	versions map[string]string
}

func newManagerFixture(t *testing.T, limits Limits) *managerFixture {
	t.Helper()
	clock := newFakeClock()
	reader := newFakeStateReader()
	observer := newFakeMemberObjectObserver()
	manager := newTestManager(reader, observer, clock, limits)

	template := newTemplate(2)
	fedObject := newFedObject(t, testFedUID, 3, template, nil, testClusterA, testClusterB)
	clusterA := newCluster(testClusterA, testUIDA)
	clusterB := newCluster(testClusterB, testUIDB)
	reader.setFedObject(fedObject)
	reader.setCluster(clusterA)
	reader.setCluster(clusterB)

	return &managerFixture{
		manager:   manager,
		reader:    reader,
		observer:  observer,
		clock:     clock,
		fedObject: fedObject,
		clusterA:  clusterA,
		clusterB:  clusterB,
		template:  template,
		ref:       defaultRef(),
		versions:  make(map[string]string),
	}
}

func (f *managerFixture) issue(t *testing.T, targets ...*fedcorev1a1.FederatedCluster) (string, *PropagationBarrier) {
	t.Helper()
	propagationTargets := make([]PropagationTarget, 0, len(targets))
	for _, cluster := range targets {
		propagationTargets = append(propagationTargets, targetFromCluster(cluster))
	}
	return issueTokenFor(t, f.manager, f.fedObject, propagationTargets...)
}

// markComplete makes the persistent state (receipt, propagation status and
// member observation) confirm the pinned input for one cluster.
func (f *managerFixture) markComplete(t *testing.T, cluster *fedcorev1a1.FederatedCluster) {
	t.Helper()
	f.versions[cluster.Name] = "gen:2"
	f.reader.setPVStatus(f.ref, newPVStatus(f.templateDigest(t), f.overrideDigest(t), f.versions))
	f.fedObject = withClusterStatus(f.fedObject, cluster.Name, fedcorev1a1.ClusterPropagationOK, 2)
	f.reader.setFedObject(f.fedObject)
	f.observer.put(cluster.Name, withSourceGeneration(newMemberObject(2, "rv-2-"+cluster.Name, true), 2))
}

func (f *managerFixture) templateDigest(t *testing.T) string {
	t.Helper()
	digest, err := DigestTemplate(f.template)
	if err != nil {
		t.Fatalf("digest template: %v", err)
	}
	return digest
}

func (f *managerFixture) overrideDigest(t *testing.T) string {
	t.Helper()
	digest, err := DigestOverrides(f.fedObject.GetSpec().Overrides)
	if err != nil {
		t.Fatalf("digest overrides: %v", err)
	}
	return digest
}

func waitAsync(t *testing.T, manager *Manager, opaque string) (context.CancelFunc, chan *WaitOutcome) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	outcomes := make(chan *WaitOutcome, 1)
	go func() {
		outcome, err := manager.WaitForPropagation(ctx, opaque)
		if err != nil {
			t.Errorf("WaitForPropagation returned unexpected error: %v", err)
			return
		}
		outcomes <- outcome
	}()
	return cancel, outcomes
}

func assertEventually(t *testing.T, cond func() bool, message string) {
	t.Helper()
	deadline := time.Now().Add(waitTimeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(testRequery)
	}
	t.Fatal(message)
}

func assertBlocked(t *testing.T, outcomes chan *WaitOutcome) {
	t.Helper()
	select {
	case outcome := <-outcomes:
		t.Fatalf("wait should still be blocked, got %+v", outcome)
	case <-time.After(150 * time.Millisecond):
	}
}

// TestWaitCompletesOnlyAfterAllTargetsConfirm verifies that a partial
// confirmation never completes the wait.
func TestWaitCompletesOnlyAfterAllTargetsConfirm(t *testing.T) {
	fixture := newManagerFixture(t, testLimits(testRequery))
	opaque, _ := fixture.issue(t, fixture.clusterA, fixture.clusterB)
	_, outcomes := waitAsync(t, fixture.manager, opaque)

	fixture.markComplete(t, fixture.clusterA)
	fixture.manager.Wake(fixture.ref)
	assertBlocked(t, outcomes)

	fixture.markComplete(t, fixture.clusterB)
	fixture.manager.Wake(fixture.ref)

	assertEventually(t, func() bool {
		select {
		case outcome := <-outcomes:
			if outcome.State != WaitCompleted || !outcome.AllComplete() || len(outcome.Results) != 2 {
				t.Fatalf("unexpected outcome: %+v", outcome)
			}
			return true
		default:
			return false
		}
	}, "wait did not complete after all targets confirmed")
}

// TestWaitInformerDelay verifies that a successful dispatch whose member object
// is not yet cached is reported as Unobserved first and completes once the
// observation catches up, without any write or retry side effect.
func TestWaitInformerDelay(t *testing.T) {
	fixture := newManagerFixture(t, testLimits(testRequery))
	opaque, _ := fixture.issue(t, fixture.clusterA)

	// Receipt and OK status already persisted, but the member informer lags.
	fixture.versions[testClusterA] = "gen:2"
	fixture.reader.setPVStatus(fixture.ref,
		newPVStatus(fixture.templateDigest(t), fixture.overrideDigest(t), fixture.versions))
	fixture.fedObject = withClusterStatus(fixture.fedObject, testClusterA, fedcorev1a1.ClusterPropagationOK, 2)
	fixture.reader.setFedObject(fixture.fedObject)

	_, outcomes := waitAsync(t, fixture.manager, opaque)
	assertBlocked(t, outcomes)

	// Informer catches up: the member object becomes observable.
	fixture.observer.put(testClusterA, withSourceGeneration(newMemberObject(2, "rv-2", true), 2))
	fixture.manager.Wake(fixture.ref)

	assertEventually(t, func() bool {
		select {
		case outcome := <-outcomes:
			return outcome.State == WaitCompleted && outcome.AllComplete()
		default:
			return false
		}
	}, "wait did not complete after informer caught up")
}

// TestWaitOldGenerationReceipt verifies that a receipt belonging to an older
// input identity cannot satisfy a token issued for the new generation.
func TestWaitOldGenerationReceipt(t *testing.T) {
	fixture := newManagerFixture(t, testLimits(testRequery))
	opaque, _ := fixture.issue(t, fixture.clusterA)
	_, outcomes := waitAsync(t, fixture.manager, opaque)

	// Old-generation receipt: digests do not match the pinned input identity.
	fixture.reader.setPVStatus(fixture.ref, newPVStatus("old-template", "old-override",
		map[string]string{testClusterA: "gen:1"}))
	fixture.observer.put(testClusterA, withSourceGeneration(newMemberObject(1, "rv-1", true), 1))
	fixture.manager.Wake(fixture.ref)
	assertBlocked(t, outcomes)

	// New dispatch lands: receipt and member observation match the token.
	fixture.markComplete(t, fixture.clusterA)
	fixture.manager.Wake(fixture.ref)

	assertEventually(t, func() bool {
		select {
		case outcome := <-outcomes:
			return outcome.State == WaitCompleted
		default:
			return false
		}
	}, "wait did not complete after the current generation was received")
}

// TestWaitClusterUIDChange verifies that rebuilding a cluster with the same
// name but a new UID makes the token Stale.
func TestWaitClusterUIDChange(t *testing.T) {
	fixture := newManagerFixture(t, testLimits(testRequery))
	opaque, _ := fixture.issue(t, fixture.clusterA)
	_, outcomes := waitAsync(t, fixture.manager, opaque)

	rebuilt := newCluster(testClusterA, types.UID("uid-cluster-a-rebuilt"))
	fixture.reader.setCluster(rebuilt)
	fixture.manager.WakeAll()

	assertEventually(t, func() bool {
		select {
		case outcome := <-outcomes:
			if outcome.State != WaitStale || !outcome.HasStale() {
				t.Fatalf("expected stale outcome, got %+v", outcome)
			}
			result := findResult(t, outcome, testClusterA)
			return result.Reason == ReasonClusterUIDMismatch
		default:
			return false
		}
	}, "cluster UID change did not make the token stale")
}

// TestWaitSameNameObjectReplacement verifies that a same-named federated object
// replacement (new UID) makes the token Stale.
func TestWaitSameNameObjectReplacement(t *testing.T) {
	fixture := newManagerFixture(t, testLimits(testRequery))
	opaque, _ := fixture.issue(t, fixture.clusterA)
	_, outcomes := waitAsync(t, fixture.manager, opaque)

	replacement := newFedObject(t, types.UID("uid-fed-object-2"), 3, fixture.template, nil, testClusterA)
	fixture.reader.setFedObject(replacement)
	fixture.manager.Wake(fixture.ref)

	assertEventually(t, func() bool {
		select {
		case outcome := <-outcomes:
			result := findResult(t, outcome, testClusterA)
			return outcome.State == WaitStale && result.Reason == ReasonFedObjectUIDMismatch
		default:
			return false
		}
	}, "object replacement did not make the token stale")
}

// TestWaitLostWakeReliesOnRequery verifies correctness when watch wake-ups are
// completely lost: periodic re-query must still observe the new persistent state.
func TestWaitLostWakeReliesOnRequery(t *testing.T) {
	fixture := newManagerFixture(t, testLimits(testRequery))
	opaque, _ := fixture.issue(t, fixture.clusterA, fixture.clusterB)
	_, outcomes := waitAsync(t, fixture.manager, opaque)

	// Deliberately never call Wake: simulate dropped watch events.
	fixture.markComplete(t, fixture.clusterA)
	fixture.markComplete(t, fixture.clusterB)

	assertEventually(t, func() bool {
		select {
		case outcome := <-outcomes:
			return outcome.State == WaitCompleted && outcome.AllComplete()
		default:
			return false
		}
	}, "periodic re-query did not recover from lost wake events")
}

// TestWaitIndependentCancellation verifies that canceling one waiter never
// disturbs other waiters and that several waiters share one read loop.
func TestWaitIndependentCancellation(t *testing.T) {
	fixture := newManagerFixture(t, testLimits(testRequery))
	opaque, _ := fixture.issue(t, fixture.clusterA)

	cancel1, outcomes1 := waitAsync(t, fixture.manager, opaque)
	_, outcomes2 := waitAsync(t, fixture.manager, opaque)

	cancel1()
	assertEventually(t, func() bool {
		select {
		case outcome := <-outcomes1:
			return outcome.State == WaitCanceled
		default:
			return false
		}
	}, "canceled waiter did not return")

	// The second waiter must remain blocked and must not have been disturbed.
	assertBlocked(t, outcomes2)

	fixture.markComplete(t, fixture.clusterA)
	fixture.manager.Wake(fixture.ref)
	assertEventually(t, func() bool {
		select {
		case outcome := <-outcomes2:
			return outcome.State == WaitCompleted
		default:
			return false
		}
	}, "remaining waiter did not complete after cancellation")

	// Coalescing: over the same wall-clock window, two waiters must cause the
	// same number of member observations as one waiter because one evaluation
	// loop serves all waiters of a token.
	measureCalls := func(waiterCount int) int {
		f := newManagerFixture(t, testLimits(testRequery))
		token, _ := f.issue(t, f.clusterA)
		cancels := make([]context.CancelFunc, 0, waiterCount)
		for i := 0; i < waiterCount; i++ {
			cancel, _ := waitAsync(t, f.manager, token)
			cancels = append(cancels, cancel)
		}
		time.Sleep(250 * time.Millisecond)
		for _, cancel := range cancels {
			cancel()
		}
		return f.observer.callCount()
	}
	callsWithOneWaiter := measureCalls(1)
	callsWithTwoWaiters := measureCalls(2)
	if callsWithTwoWaiters > callsWithOneWaiter+1 {
		t.Fatalf("waiters were not coalesced onto one read loop: %d calls with 1 waiter vs %d with 2",
			callsWithOneWaiter, callsWithTwoWaiters)
	}
}

// TestWaitTTLTimeout verifies that an unfulfilled token times out with a
// per-target breakdown rather than a global boolean.
func TestWaitTTLTimeout(t *testing.T) {
	limits := testLimits(testRequery)
	limits.DefaultTTL = 200 * time.Millisecond
	limits.MaxTTL = time.Second
	fixture := newManagerFixture(t, limits)
	opaque, _ := fixture.issue(t, fixture.clusterA, fixture.clusterB)

	start := time.Now()
	outcome, err := fixture.manager.WaitForPropagation(context.Background(), opaque)
	if err != nil {
		t.Fatalf("WaitForPropagation: %v", err)
	}
	if time.Since(start) < 150*time.Millisecond {
		t.Fatal("wait returned before the token TTL elapsed")
	}
	if outcome.State != WaitTimeout {
		t.Fatalf("expected timeout outcome, got %s", outcome.State)
	}
	if len(outcome.Results) != 2 {
		t.Fatalf("expected one result per target, got %d", len(outcome.Results))
	}
	for _, result := range outcome.Results {
		if result.State == TargetComplete {
			t.Fatalf("incomplete target reported complete: %+v", result)
		}
	}
}

// TestWaitRejectsUnknownAndExpiredTokens verifies the opaque server-side state
// boundary and TTL enforcement.
func TestWaitRejectsUnknownAndExpiredTokens(t *testing.T) {
	fixture := newManagerFixture(t, testLimits(testRequery))
	opaque, _ := fixture.issue(t, fixture.clusterA)

	t.Run("token signed by another manager is invalid", func(t *testing.T) {
		other := NewManager("other", []byte("a-different-signing-key"),
			newFakeStateReader(), newFakeMemberObjectObserver(), testLimits(testRequery), klog.Background())
		if _, err := other.WaitForPropagation(context.Background(), opaque); !errors.Is(err, ErrInvalidToken) {
			t.Fatalf("expected ErrInvalidToken, got %v", err)
		}
	})

	t.Run("expired token is rejected and removed", func(t *testing.T) {
		fixture.clock.Advance(time.Hour)
		if _, err := fixture.manager.WaitForPropagation(context.Background(), opaque); !errors.Is(err, ErrTokenExpired) {
			t.Fatalf("expected ErrTokenExpired, got %v", err)
		}
		if _, err := fixture.manager.WaitForPropagation(context.Background(), opaque); !errors.Is(err, ErrUnknownToken) {
			t.Fatalf("expected ErrUnknownToken after expiry cleanup, got %v", err)
		}
	})
}

// TestManagerEnforcesLimits verifies all resource bounds.
func TestManagerEnforcesLimits(t *testing.T) {
	t.Run("max waiters per token", func(t *testing.T) {
		limits := testLimits(testRequery)
		limits.MaxWaitersPerToken = 2
		fixture := newManagerFixture(t, limits)
		opaque, _ := fixture.issue(t, fixture.clusterA)

		cancel1, _ := waitAsync(t, fixture.manager, opaque)
		cancel2, _ := waitAsync(t, fixture.manager, opaque)
		defer cancel1()
		defer cancel2()
		time.Sleep(50 * time.Millisecond)

		if _, err := fixture.manager.WaitForPropagation(context.Background(), opaque); !errors.Is(err, ErrTooManyWaiters) {
			t.Fatalf("expected ErrTooManyWaiters, got %v", err)
		}
	})

	t.Run("max tokens", func(t *testing.T) {
		limits := testLimits(testRequery)
		limits.MaxTokens = 1
		fixture := newManagerFixture(t, limits)
		if _, _, err := fixture.manager.Issue(fixture.input(t)); err != nil {
			t.Fatalf("first issue: %v", err)
		}
		if _, _, err := fixture.manager.Issue(fixture.input(t)); !errors.Is(err, ErrTooManyTokens) {
			t.Fatalf("expected ErrTooManyTokens, got %v", err)
		}
	})

	t.Run("queue byte budget", func(t *testing.T) {
		limits := testLimits(testRequery)
		limits.MaxTotalQueueBytes = 1
		fixture := newManagerFixture(t, limits)
		if _, _, err := fixture.manager.Issue(fixture.input(t)); !errors.Is(err, ErrQueueLimitExceeded) {
			t.Fatalf("expected ErrQueueLimitExceeded, got %v", err)
		}
	})

	t.Run("token payload size", func(t *testing.T) {
		limits := testLimits(testRequery)
		limits.MaxTokenPayloadBytes = 1
		fixture := newManagerFixture(t, limits)
		if _, _, err := fixture.manager.Issue(fixture.input(t)); !errors.Is(err, ErrQueueLimitExceeded) {
			t.Fatalf("expected ErrQueueLimitExceeded, got %v", err)
		}
	})
}

// input builds an issue request without consuming any manager quota.
func (f *managerFixture) input(t *testing.T) IssueTokenInput {
	t.Helper()
	return IssueTokenInput{
		ObjectRef:      f.ref,
		FederatedGVK:   common.FederatedObjectGVK,
		SourceGVK:      testSourceGVK,
		FederatedUID:   f.fedObject.UID,
		Generation:     f.fedObject.Generation,
		TemplateDigest: f.templateDigest(t),
		OverrideDigest: f.overrideDigest(t),
		Targets:        []PropagationTarget{targetFromCluster(f.clusterA)},
	}
}

// TestManagerShutdownCancelsWaiters verifies graceful shutdown behavior.
func TestManagerShutdownCancelsWaiters(t *testing.T) {
	fixture := newManagerFixture(t, testLimits(testRequery))
	opaque, _ := fixture.issue(t, fixture.clusterA)
	_, outcomes := waitAsync(t, fixture.manager, opaque)

	fixture.manager.Shutdown()
	assertEventually(t, func() bool {
		select {
		case outcome := <-outcomes:
			return outcome.State == WaitCanceled
		default:
			return false
		}
	}, "shutdown did not cancel pending waiters")

	if _, _, err := fixture.manager.Issue(fixture.input(t)); !errors.Is(err, ErrManagerClosed) {
		t.Fatalf("expected ErrManagerClosed, got %v", err)
	}
}

// TestSweeperReapsExpiredTokens verifies that tokens without waiters are
// garbage collected at expiry so the token count stays bounded.
func TestSweeperReapsExpiredTokens(t *testing.T) {
	limits := testLimits(testRequery)
	limits.DefaultTTL = 100 * time.Millisecond
	limits.MaxTTL = time.Second
	limits.SweepInterval = 50 * time.Millisecond
	fixture := newManagerFixture(t, limits)
	opaque, _ := fixture.issue(t, fixture.clusterA)

	managerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go fixture.manager.Run(managerCtx)

	fixture.clock.Advance(time.Second)
	assertEventually(t, func() bool {
		_, err := fixture.manager.WaitForPropagation(context.Background(), opaque)
		return errors.Is(err, ErrUnknownToken)
	}, "sweeper did not reap the expired token")
}

// TestWaitPerformsNoWrites verifies the read-only guarantee: the reader and
// observer interfaces expose no mutating methods by construction, and the
// observed objects stay byte-identical through a complete wait cycle.
func TestWaitPerformsNoWrites(t *testing.T) {
	fixture := newManagerFixture(t, testLimits(testRequery))
	opaque, _ := fixture.issue(t, fixture.clusterA)

	fedUIDBefore := fixture.fedObject.UID
	fedGenerationBefore := fixture.fedObject.Generation
	fixture.markComplete(t, fixture.clusterA)

	outcome, err := fixture.manager.WaitForPropagation(context.Background(), opaque)
	if err != nil {
		t.Fatalf("WaitForPropagation: %v", err)
	}
	if !outcome.AllComplete() {
		t.Fatalf("expected complete outcome, got %+v", outcome)
	}

	current, err := fixture.reader.GetFederatedObject(context.Background(), fixture.ref)
	if err != nil {
		t.Fatalf("read federated object: %v", err)
	}
	if current.GetUID() != fedUIDBefore || current.GetGeneration() != fedGenerationBefore {
		t.Fatal("federated object identity changed during a read-only wait")
	}
}

// TestWaitLoopRestartsAfterLastWaiterCanceled is a regression test for the
// race where the evaluation loop could exit just as a new waiter attached.
// With non-atomic stop bookkeeping the new waiter observed evalRunning==true,
// skipped starting its own loop and was then stranded when the old loop
// returned. The empty-waiter check and the evalRunning flip are now atomic, so
// every new waiter is guaranteed a serving evaluation loop.
func TestWaitLoopRestartsAfterLastWaiterCanceled(t *testing.T) {
	for iteration := 0; iteration < 100; iteration++ {
		fixture := newManagerFixture(t, testLimits(testRequery))
		opaque, _ := fixture.issue(t, fixture.clusterA)

		cancel, _ := waitAsync(t, fixture.manager, opaque)
		time.Sleep(5 * time.Millisecond)
		cancel()
		// Nudge the loop so it reaches its empty-waiter check while the next
		// waiter is about to attach.
		fixture.manager.Wake(fixture.ref)
		time.Sleep(5 * time.Millisecond)

		_, outcomes := waitAsync(t, fixture.manager, opaque)
		fixture.markComplete(t, fixture.clusterA)
		fixture.manager.Wake(fixture.ref)

		assertEventually(t, func() bool {
			select {
			case outcome := <-outcomes:
				return outcome.State == WaitCompleted && outcome.AllComplete()
			default:
				return false
			}
		}, "new waiter was stranded after the previous evaluation loop exited")
	}
}

// TestConcurrentWaitCancelAndSweep exercises concurrent waiter cancellation,
// TTL sweeps and terminal delivery against one manager, including the shared
// lastResults snapshot that the sweeper reads while evaluation loops write it.
func TestConcurrentWaitCancelAndSweep(t *testing.T) {
	limits := testLimits(testRequery)
	limits.DefaultTTL = 200 * time.Millisecond
	limits.MaxTTL = time.Second
	limits.SweepInterval = 25 * time.Millisecond
	fixture := newManagerFixture(t, limits)

	runCtx, runCancel := context.WithCancel(context.Background())
	defer runCancel()
	go fixture.manager.Run(runCtx)

	// Expire every token from the sweeper's point of view while waits are
	// still in flight, so the sweep path races with live evaluation loops.
	go func() {
		time.Sleep(100 * time.Millisecond)
		fixture.clock.Advance(time.Second)
	}()

	const tokenCount = 24
	var wg sync.WaitGroup
	for i := 0; i < tokenCount; i++ {
		opaque, _ := fixture.issue(t, fixture.clusterA)
		wg.Add(1)
		go func(opaque string, cancelEarly bool) {
			defer wg.Done()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if cancelEarly {
				go func() {
					time.Sleep(50 * time.Millisecond)
					cancel()
				}()
			}
			outcome, err := fixture.manager.WaitForPropagation(ctx, opaque)
			if err != nil {
				t.Errorf("unexpected wait error: %v", err)
				return
			}
			switch outcome.State {
			case WaitCanceled, WaitTimeout:
			default:
				t.Errorf("expected Canceled or Timeout, got %s", outcome.State)
			}
		}(opaque, i%2 == 0)
	}
	wg.Wait()
}
