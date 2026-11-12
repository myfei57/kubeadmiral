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
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
)

// waiter represents one WaitForPropagation caller. Every waiter has its own
// result channel, so cancellation is always independent even when several
// waiters share one evaluation loop.
type waiter struct {
	id     int64
	result chan WaitOutcome
}

// trackedToken is the manager's opaque server-side state for one token.
type trackedToken struct {
	barrier *PropagationBarrier
	// wake is a coalescing, fixed-capacity channel. A full channel simply drops
	// the extra signal because the periodic re-query makes every wake best-effort.
	wake chan struct{}

	waiters      map[int64]*waiter
	nextWaiterID int64
	evalRunning  bool
	lastResults  []TargetResult

	tokenBytes int
}

// Manager issues PropagationBarrier tokens and serves WaitForPropagation calls.
// It keeps bounded in-memory state only and performs reads exclusively through
// the supplied StateReader and MemberObjectObserver; it never writes to the
// API and never retries business operations.
type Manager struct {
	name       string
	signingKey []byte
	limits     Limits
	reader     StateReader
	observer   MemberObjectObserver
	logger     klog.Logger
	now        func() time.Time

	mu         sync.Mutex
	tokens     map[string]*trackedToken
	queueBytes int
	closed     bool

	done     chan struct{}
	stopOnce sync.Once
}

// NewManager creates a barrier manager. The signingKey must remain stable for
// the lifetime of the manager because tokens are verified against it; a random
// per-process key is appropriate given that all waiter state is in-memory.
func NewManager(
	name string,
	signingKey []byte,
	reader StateReader,
	observer MemberObjectObserver,
	limits Limits,
	logger klog.Logger,
) *Manager {
	if logger.GetSink() == nil {
		logger = klog.Background()
	}
	if len(signingKey) == 0 {
		signingKey = randomSigningKey()
	}
	return &Manager{
		name:       name,
		signingKey: signingKey,
		limits:     limits.withDefaults(),
		reader:     reader,
		observer:   observer,
		logger:     logger.WithValues("component", "propagation-barrier-manager", "manager", name),
		now:        time.Now,
		tokens:     make(map[string]*trackedToken),
		done:       make(chan struct{}),
	}
}

// detachContext derives a context from the waiter's context while detaching
// its cancellation and deadline, because the shared evaluation loop outlives
// any individual waiter. It is canceled when Shutdown closes the done channel
// or when the loop itself returns.
func (m *Manager) detachContext(parent context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.WithoutCancel(parent))
	go func() {
		select {
		case <-m.done:
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}

// Issue validates the input, signs and registers a new barrier token and
// returns its opaque encoding. The token is immutable once issued.
func (m *Manager) Issue(input IssueTokenInput) (string, *PropagationBarrier, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return "", nil, ErrManagerClosed
	}

	barrier, err := newPropagationBarrier(input, m.now(), m.limits)
	if err != nil {
		return "", nil, err
	}
	opaque, err := encodeToken(barrier, m.signingKey)
	if err != nil {
		return "", nil, err
	}
	if len(opaque) > m.limits.MaxTokenPayloadBytes {
		return "", nil, ErrQueueLimitExceeded
	}
	if len(m.tokens) >= m.limits.MaxTokens {
		return "", nil, ErrTooManyTokens
	}

	cost := len(opaque) + m.limits.tokenOverheadBytes + m.limits.wakeBufferSlots
	if m.queueBytes+cost > m.limits.MaxTotalQueueBytes {
		return "", nil, ErrQueueLimitExceeded
	}

	m.tokens[barrier.TokenID] = &trackedToken{
		barrier:    barrier,
		wake:       make(chan struct{}, m.limits.wakeBufferSlots),
		waiters:    make(map[int64]*waiter),
		tokenBytes: cost,
	}
	m.queueBytes += cost

	m.logger.V(2).Info("Issued propagation barrier token",
		"token", barrier.TokenID, "object", barrier.ObjectRef.String(), "targets", len(barrier.Targets))
	return opaque, barrier, nil
}

// WaitForPropagation blocks until every target of the token confirms the pinned
// input identity, the token becomes Stale, the TTL elapses, the caller context
// ends or the manager shuts down. Reads are shared between waiters of the same
// token but each caller cancels independently. The call performs no writes.
func (m *Manager) WaitForPropagation(ctx context.Context, opaque string) (*WaitOutcome, error) {
	barrier, err := parseAndVerifyToken(opaque, m.signingKey)
	if err != nil {
		return nil, err
	}

	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		// A wait that races with shutdown is canceled rather than rejected:
		// the caller did nothing wrong and still gets a structured outcome.
		return &WaitOutcome{
			TokenID:    barrier.TokenID,
			State:      WaitCanceled,
			Results:    m.synthResults(barrier, ReasonTokenExpired, "manager shutting down"),
			ObservedAt: metav1.NewTime(m.now()),
		}, nil
	}
	tt, ok := m.tokens[barrier.TokenID]
	if !ok {
		m.mu.Unlock()
		return nil, ErrUnknownToken
	}
	if barrier.Expired(m.now()) {
		m.deleteTokenLocked(tt)
		m.mu.Unlock()
		return nil, ErrTokenExpired
	}
	if len(tt.waiters) >= m.limits.MaxWaitersPerToken {
		m.mu.Unlock()
		return nil, ErrTooManyWaiters
	}
	if m.queueBytes+m.limits.perWaiterOverheadBytes > m.limits.MaxTotalQueueBytes {
		m.mu.Unlock()
		return nil, ErrQueueLimitExceeded
	}

	id := tt.nextWaiterID
	tt.nextWaiterID++
	w := &waiter{id: id, result: make(chan WaitOutcome, 1)}
	tt.waiters[id] = w
	m.queueBytes += m.limits.perWaiterOverheadBytes
	startLoop := !tt.evalRunning
	if startLoop {
		tt.evalRunning = true
	}
	m.mu.Unlock()

	if startLoop {
		loopCtx, cancel := m.detachContext(ctx)
		go func() {
			defer cancel()
			m.evalLoop(loopCtx, tt)
		}()
	}

	// Independent cancellation: removing the waiter never affects other
	// waiters attached to the same token.
	go func() {
		select {
		case <-ctx.Done():
			m.removeWaiter(tt, id, WaitOutcome{
				TokenID:    barrier.TokenID,
				State:      WaitCanceled,
				ObservedAt: metav1.NewTime(m.now()),
			})
		case <-w.result:
		case <-m.done:
		}
	}()

	select {
	case outcome := <-w.result:
		return &outcome, nil
	case <-ctx.Done():
		select {
		case outcome := <-w.result:
			return &outcome, nil
		default:
			return &WaitOutcome{
				TokenID:    barrier.TokenID,
				State:      WaitCanceled,
				ObservedAt: metav1.NewTime(m.now()),
			}, nil
		}
	case <-m.done:
		return &WaitOutcome{
			TokenID:    barrier.TokenID,
			State:      WaitCanceled,
			Results:    m.synthResults(tt.barrier, ReasonTokenExpired, "manager shutting down"),
			ObservedAt: metav1.NewTime(m.now()),
		}, nil
	}
}

// Wake nudges every evaluation loop waiting on the given federated object.
// Watch handlers call this; it is only a wake-up signal and may be dropped when
// the coalesced channel is full. Correctness never depends on it because the
// periodic re-query observes the same persistent state eventually.
func (m *Manager) Wake(ref ObjectRef) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, tt := range m.tokens {
		if tt.barrier.ObjectRef == ref {
			m.nudgeLocked(tt)
		}
	}
}

// WakeAll nudges every token. It is used for cluster-wide events such as a
// FederatedCluster update, which can affect tokens targeting different objects.
func (m *Manager) WakeAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, tt := range m.tokens {
		m.nudgeLocked(tt)
	}
}

func (m *Manager) nudgeLocked(tt *trackedToken) {
	select {
	case tt.wake <- struct{}{}:
	default:
		// Coalesce: a wake is already pending or the loop is evaluating.
	}
}

// Run starts the TTL sweeper. It blocks until ctx is canceled and then shuts
// the manager down, delivering WaitCanceled to every remaining waiter.
func (m *Manager) Run(ctx context.Context) {
	sweeper := time.NewTicker(m.limits.SweepInterval)
	defer sweeper.Stop()
	for {
		select {
		case <-ctx.Done():
			m.Shutdown()
			return
		case <-sweeper.C:
			m.sweepExpired()
		}
	}
}

// Shutdown terminates all evaluation loops and wakes. It is safe to call more
// than once.
func (m *Manager) Shutdown() {
	m.stopOnce.Do(func() {
		close(m.done)

		m.mu.Lock()
		pending := make([]*trackedToken, 0, len(m.tokens))
		for _, tt := range m.tokens {
			pending = append(pending, tt)
		}
		m.closed = true
		m.mu.Unlock()

		for _, tt := range pending {
			m.finishToken(tt, WaitOutcome{
				TokenID:    tt.barrier.TokenID,
				State:      WaitCanceled,
				Results:    m.lastOrSynthResults(tt),
				ObservedAt: metav1.NewTime(m.now()),
			})
		}
	})
}

func (m *Manager) sweepExpired() {
	now := m.now()
	m.mu.Lock()
	var expired []*trackedToken
	for _, tt := range m.tokens {
		if tt.barrier.Expired(now) {
			expired = append(expired, tt)
		}
	}
	m.mu.Unlock()

	for _, tt := range expired {
		m.logger.V(2).Info("Propagation barrier token expired", "token", tt.barrier.TokenID)
		m.finishToken(tt, WaitOutcome{
			TokenID:    tt.barrier.TokenID,
			State:      WaitTimeout,
			Results:    m.lastOrSynthResults(tt),
			ObservedAt: metav1.NewTime(now),
		})
	}
}

// evalLoop runs at most one per token. It evaluates immediately on start and
// after every wake or re-query tick, and exits once the token reaches a
// terminal state, expires, loses all waiters or the manager shuts down.
func (m *Manager) evalLoop(ctx context.Context, tt *trackedToken) {
	deadline := time.NewTimer(tt.barrier.ExpiresAt.Sub(m.now()))
	defer deadline.Stop()
	requery := time.NewTicker(m.limits.RequeryInterval)
	defer requery.Stop()

	for {
		// The empty-waiter check and the evalRunning flip must happen in one
		// critical section. Otherwise a waiter attaching in the window between
		// the two operations could observe evalRunning==true, skip starting a
		// loop, and then watch this loop exit, leaving the new waiter stranded
		// until the TTL sweep.
		m.mu.Lock()
		shouldStop := m.tryStopEvalLocked(tt)
		m.mu.Unlock()
		if shouldStop {
			// All callers canceled; stop reading until a new waiter arrives
			// and starts a fresh evaluation loop.
			return
		}

		outcome, terminal := m.evaluateOnce(ctx, tt)
		if terminal {
			m.finishToken(tt, outcome)
			return
		}

		select {
		case <-tt.wake:
			// Re-evaluate immediately; lost or coalesced wakes are covered by
			// the requery ticker below.
		case <-requery.C:
		case <-deadline.C:
			finalOutcome, finalTerminal := m.evaluateOnce(ctx, tt)
			if finalTerminal {
				m.finishToken(tt, finalOutcome)
				return
			}
			finalOutcome.State = WaitTimeout
			finalOutcome.ObservedAt = metav1.NewTime(m.now())
			m.finishToken(tt, finalOutcome)
			return
		case <-m.done:
			return
		}
	}
}

// evaluateOnce takes one shared snapshot and classifies all targets. Read
// failures are non-terminal: they are reported per target and retried on the
// next wake or re-query without triggering any write or business retry.
func (m *Manager) evaluateOnce(ctx context.Context, tt *trackedToken) (WaitOutcome, bool) {
	readCtx, cancel := context.WithTimeout(ctx, m.limits.RequeryInterval)
	defer cancel()

	snap, err := takeSnapshot(readCtx, m.reader, m.observer, tt.barrier)
	now := m.now()
	if err != nil {
		m.logger.Error(err, "Failed to take propagation barrier snapshot", "token", tt.barrier.TokenID)
		results := m.synthResults(tt.barrier, ReasonMemberRetrievalFailed, err.Error())
		return WaitOutcome{
			TokenID:    tt.barrier.TokenID,
			Results:    results,
			ObservedAt: metav1.NewTime(now),
		}, false
	}

	results := evaluate(tt.barrier, snap, now)
	// lastResults is read concurrently by the sweeper and Shutdown, so the
	// write is guarded by the manager mutex.
	m.setLastResults(tt, results)

	outcome := WaitOutcome{
		TokenID:    tt.barrier.TokenID,
		Results:    results,
		ObservedAt: metav1.NewTime(now),
	}
	switch {
	case allComplete(results):
		outcome.State = WaitCompleted
		return outcome, true
	case hasStale(results):
		outcome.State = WaitStale
		return outcome, true
	default:
		return outcome, false
	}
}

func (m *Manager) removeWaiter(tt *trackedToken, id int64, outcome WaitOutcome) {
	m.mu.Lock()
	w, ok := tt.waiters[id]
	if !ok {
		m.mu.Unlock()
		return
	}
	delete(tt.waiters, id)
	m.queueBytes -= m.limits.perWaiterOverheadBytes
	if m.queueBytes < 0 {
		m.queueBytes = 0
	}
	m.mu.Unlock()

	select {
	case w.result <- outcome:
	default:
	}
}

// finishToken fans a terminal outcome out to every waiter, removes the token
// and releases its byte budget. Exactly one of finishToken, removeWaiter and
// Shutdown delivers to any given waiter.
func (m *Manager) finishToken(tt *trackedToken, outcome WaitOutcome) {
	m.mu.Lock()
	current, ok := m.tokens[tt.barrier.TokenID]
	if ok && current == tt {
		delete(m.tokens, tt.barrier.TokenID)
		m.queueBytes -= tt.tokenBytes
		if m.queueBytes < 0 {
			m.queueBytes = 0
		}
	}
	waiters := tt.waiters
	tt.waiters = make(map[int64]*waiter)
	tt.evalRunning = false
	m.queueBytes -= len(waiters) * m.limits.perWaiterOverheadBytes
	if m.queueBytes < 0 {
		m.queueBytes = 0
	}
	m.mu.Unlock()

	for _, w := range waiters {
		select {
		case w.result <- outcome:
		default:
		}
	}
}

func (m *Manager) deleteTokenLocked(tt *trackedToken) {
	delete(m.tokens, tt.barrier.TokenID)
	m.queueBytes -= tt.tokenBytes
	if m.queueBytes < 0 {
		m.queueBytes = 0
	}
}

// tryStopEvalLocked atomically stops the evaluation loop when it has lost all
// waiters. Combining the empty-waiter check and the evalRunning flip in one
// critical section prevents a newly attached waiter from being stranded by a
// loop that exits in the middle of the waiter's attachment. The caller must
// hold m.mu.
func (m *Manager) tryStopEvalLocked(tt *trackedToken) bool {
	if len(tt.waiters) == 0 {
		tt.evalRunning = false
		return true
	}
	return false
}

func (m *Manager) setLastResults(tt *trackedToken, results []TargetResult) {
	m.mu.Lock()
	defer m.mu.Unlock()
	tt.lastResults = results
}

func (m *Manager) lastOrSynthResults(tt *trackedToken) []TargetResult {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(tt.lastResults) == len(tt.barrier.Targets) {
		return tt.lastResults
	}
	return m.synthResults(tt.barrier, ReasonMemberObjectNotObserved, "token ended before all targets were observed")
}

// synthResults guarantees one result entry per token target even when no
// successful snapshot was ever taken.
func (m *Manager) synthResults(barrier *PropagationBarrier, reason, message string) []TargetResult {
	results := make([]TargetResult, 0, len(barrier.Targets))
	for _, target := range barrier.Targets {
		results = append(results, TargetResult{
			ClusterName: target.ClusterName,
			ClusterUID:  target.ClusterUID,
			State:       TargetUnobserved,
			Reason:      reason,
			Message:     message,
		})
	}
	return results
}

func allComplete(results []TargetResult) bool {
	if len(results) == 0 {
		return false
	}
	for i := range results {
		if results[i].State != TargetComplete {
			return false
		}
	}
	return true
}

func hasStale(results []TargetResult) bool {
	for i := range results {
		if results[i].State == TargetStale {
			return true
		}
	}
	return false
}
