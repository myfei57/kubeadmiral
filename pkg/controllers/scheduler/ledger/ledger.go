/*
Copyright 2023 The KubeAdmiral Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package ledger implements the optional in-memory PendingPlacementLedger. The ledger reserves cluster
// resource vectors for placement demands that the current scheduler leader has committed but which have
// not yet been observably accounted for by the member clusters. It prevents multiple FederatedObjects
// from repeatedly allocating against the same stale capacity snapshot within one leader epoch.
//
// The ledger is intentionally leader-local: it only constrains the in-flight federated demands of the
// owning leader and never claims to govern other schedulers acting on member clusters locally.
package ledger

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"

	fedcorev1a1 "github.com/kubewharf/kubeadmiral/pkg/apis/core/v1alpha1"
	"github.com/kubewharf/kubeadmiral/pkg/controllers/scheduler/framework"
)

// staleAccountRatio is the fraction of MaxAccounts that may be held by TTL-expired but unprovable
// reservations before admission is paused.
const staleAccountRatio = 10

var epochCounter atomic.Uint64

func nextEpoch() uint64 {
	return uint64(time.Now().UnixNano()) + epochCounter.Add(1)
}

// reservation is a single ledger account leg.
type reservation struct {
	key         framework.LedgerKey
	clusterName string
	resources   framework.Resource
	createdAt   time.Time
	// basisRV is the ResourceVersion of the FederatedCluster snapshot observed when the account was
	// created. Member inclusion is only provable once the observed cluster RV advances past it.
	basisRV string
	stale   bool
}

type waiter struct {
	ticket uint64
	ch     chan struct{}
}

type pendingPlacementLedger struct {
	mu sync.Mutex

	config framework.LedgerConfig
	epoch  uint64
	active bool
	// rebuilding is true between leader start and the completion of the bounded rebuild.
	rebuilding bool

	accounts     map[framework.LedgerKey]*reservation
	clusterIndex map[types.UID]map[framework.LedgerKey]struct{}
	fedIndex     map[types.UID]map[framework.LedgerKey]struct{}
	clusterNames map[types.UID]string

	// snapshots is the latest observed capacity basis per cluster UID.
	snapshots map[types.UID]framework.ClusterObservation

	waitQueue  []*waiter
	nextTicket uint64

	now func() time.Time
}

var _ framework.PendingPlacementLedger = &pendingPlacementLedger{}

// NewPendingPlacementLedger creates a ledger for a new leader epoch. A fresh epoch is minted on every
// construction, so reservations from a previous leader instance can never commit under the new epoch.
func NewPendingPlacementLedger(config framework.LedgerConfig) framework.PendingPlacementLedger {
	config.Normalize()
	return &pendingPlacementLedger{
		config: config,
		epoch:  nextEpoch(),
		active: true,
		// A new leader epoch starts fail-closed and only admits placements after a bounded rebuild.
		rebuilding:   true,
		accounts:     make(map[framework.LedgerKey]*reservation),
		clusterIndex: make(map[types.UID]map[framework.LedgerKey]struct{}),
		fedIndex:     make(map[types.UID]map[framework.LedgerKey]struct{}),
		clusterNames: make(map[types.UID]string),
		snapshots:    make(map[types.UID]framework.ClusterObservation),
		now:          time.Now,
	}
}

func (l *pendingPlacementLedger) Epoch() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.epoch
}

func (l *pendingPlacementLedger) Deactivate() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.active {
		return
	}
	l.active = false
	l.signalHeadLocked()
}

// ScaleResource returns r multiplied by factor, rejecting int64 overflow. It keeps the exact integer
// Quantity representation (milli-cpu, bytes and scalar integers) used by the scheduling framework.
func ScaleResource(r framework.Resource, factor int64) (framework.Resource, error) {
	if factor < 0 {
		return framework.Resource{}, fmt.Errorf("resource scale factor must not be negative: %d", factor)
	}
	if factor == 0 {
		return framework.Resource{ScalarResources: map[corev1.ResourceName]int64{}}, nil
	}
	out := framework.Resource{}
	var err error
	if out.MilliCPU, err = checkedMultiply(r.MilliCPU, factor); err != nil {
		return framework.Resource{}, fmt.Errorf("cpu overflow: %w", err)
	}
	if out.Memory, err = checkedMultiply(r.Memory, factor); err != nil {
		return framework.Resource{}, fmt.Errorf("memory overflow: %w", err)
	}
	if out.EphemeralStorage, err = checkedMultiply(r.EphemeralStorage, factor); err != nil {
		return framework.Resource{}, fmt.Errorf("ephemeral storage overflow: %w", err)
	}
	if len(r.ScalarResources) > 0 {
		out.ScalarResources = make(map[corev1.ResourceName]int64, len(r.ScalarResources))
		for name, q := range r.ScalarResources {
			scaled, err := checkedMultiply(q, factor)
			if err != nil {
				return framework.Resource{}, fmt.Errorf("scalar resource %s overflow: %w", name, err)
			}
			out.ScalarResources[name] = scaled
		}
	}
	return out, nil
}

func checkedMultiply(v, factor int64) (int64, error) {
	if v == 0 {
		return 0, nil
	}
	if factor > 1 && v > (1<<63-1)/factor {
		return 0, fmt.Errorf("integer overflow multiplying %d by %d", v, factor)
	}
	return v * factor, nil
}

func (l *pendingPlacementLedger) Rebuild(ctx context.Context, input framework.ReconcileInput) error {
	l.mu.Lock()
	if !l.active {
		l.mu.Unlock()
		return framework.ErrLedgerInactive
	}
	l.rebuilding = true
	l.accounts = make(map[framework.LedgerKey]*reservation)
	l.clusterIndex = make(map[types.UID]map[framework.LedgerKey]struct{})
	l.fedIndex = make(map[types.UID]map[framework.LedgerKey]struct{})
	l.clusterNames = make(map[types.UID]string)
	l.snapshots = make(map[types.UID]framework.ClusterObservation, len(input.Clusters))
	l.mu.Unlock()

	// Record the current capacity basis first.
	for _, cluster := range input.Clusters {
		l.mu.Lock()
		l.snapshots[cluster.ClusterUID] = cluster
		if cluster.ClusterName != "" {
			l.clusterNames[cluster.ClusterUID] = cluster.ClusterName
		}
		l.mu.Unlock()
	}

	rebuilt := 0
	for _, obj := range input.Objects {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("ledger rebuild interrupted: %w", err)
		}
		for _, observed := range obj.Clusters {
			cluster, ok := l.findClusterByName(observed.ClusterName)
			if !ok {
				// The cluster no longer exists; nothing to reserve against.
				continue
			}
			// At rebuild the reservation basis is the currently observed cluster RV, so inclusion can never
			// be proven immediately (a later, strictly newer snapshot is required). Every unconfirmed
			// placement is therefore reconstructed conservatively. The rebuild input carries no resource
			// vectors, so a zero-vector placeholder account is recorded: it occupies the bounded
			// account/index caps and blocks unsafe optimistic reuse, while the scheduler re-reserves the
			// exact vector under a regular attempt on the next reconcile of the object.
			key := framework.LedgerKey{
				FederatedUID:        obj.UID,
				FederatedGeneration: obj.Generation,
				ClusterUID:          cluster.ClusterUID,
				Attempt:             framework.RebuildAttempt,
			}
			l.mu.Lock()
			if err := l.checkCapsLocked(len(l.accounts) + 1); err != nil {
				l.mu.Unlock()
				// Fail closed: stay in rebuilding state and let the scheduler retry.
				return err
			}
			if _, exists := l.accounts[key]; !exists {
				if l.clusterAccountCount(cluster.ClusterUID) >= l.config.MaxAccountsPerCluster ||
					l.fedAccountCount(obj.UID) >= l.config.MaxAccountsPerFederatedObject {
					l.mu.Unlock()
					return framework.ErrLedgerAccountLimit
				}
			}
			l.putLocked(&reservation{
				key:         key,
				clusterName: cluster.ClusterName,
				resources:   framework.Resource{},
				createdAt:   l.now(),
				basisRV:     cluster.ResourceVersion,
			})
			rebuilt++
			l.mu.Unlock()
		}
	}

	l.mu.Lock()
	l.rebuilding = false
	l.signalHeadLocked()
	l.mu.Unlock()
	klog.FromContext(ctx).V(2).Info("Pending placement ledger rebuild completed", "epoch", l.epoch, "rebuiltAccounts", rebuilt)
	return nil
}

func (l *pendingPlacementLedger) findClusterByName(name string) (framework.ClusterObservation, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, cluster := range l.snapshots {
		if cluster.ClusterName == name {
			return cluster, true
		}
	}
	return framework.ClusterObservation{}, false
}

func (l *pendingPlacementLedger) Reserve(ctx context.Context, plan framework.ReservationPlan) error {
	if err := validatePlan(plan); err != nil {
		return err
	}

	deadline := l.now().Add(l.config.ReserveTimeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}

	l.mu.Lock()
	if !l.active {
		l.mu.Unlock()
		return framework.ErrLedgerInactive
	}
	if l.rebuilding {
		l.mu.Unlock()
		return framework.ErrLedgerRebuilding
	}
	if len(l.waitQueue) >= l.config.MaxWaiters {
		l.mu.Unlock()
		return framework.ErrTooManyWaiters
	}
	w := &waiter{ticket: l.nextTicket, ch: make(chan struct{}, 1)}
	l.nextTicket++
	l.waitQueue = append(l.waitQueue, w)
	l.mu.Unlock()

	defer l.removeWaiter(w)

	for {
		l.mu.Lock()
		if !l.active {
			l.mu.Unlock()
			return framework.ErrLedgerInactive
		}
		if l.rebuilding {
			l.mu.Unlock()
			return framework.ErrLedgerRebuilding
		}
		if l.headTicketLocked() == w.ticket {
			err := l.tryReserveLocked(plan)
			if err == nil || !errors.Is(err, framework.ErrInsufficientReservedCapacity) {
				// Success or a non-retryable error: leave the queue either way and wake the next waiter.
				l.mu.Unlock()
				l.signalHeadAfterUnlock()
				return err
			}
		}
		l.mu.Unlock()

		waitFor := time.Until(deadline)
		if waitFor <= 0 {
			return fmt.Errorf("%w: reserve timeout for federated object %s", framework.ErrInsufficientReservedCapacity, plan.FederatedUID)
		}
		timer := time.NewTimer(waitFor)
		select {
		case <-w.ch:
			timer.Stop()
			continue
		case <-timer.C:
			return fmt.Errorf("%w: reserve timeout for federated object %s", framework.ErrInsufficientReservedCapacity, plan.FederatedUID)
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		}
	}
}

func validatePlan(plan framework.ReservationPlan) error {
	if plan.FederatedUID == "" || plan.Attempt < 1 || len(plan.Items) == 0 {
		return fmt.Errorf("%w: uid, positive attempt and at least one plan item are required", framework.ErrInvalidReservationPlan)
	}
	seen := make(map[types.UID]struct{}, len(plan.Items))
	for _, item := range plan.Items {
		if item.ClusterUID == "" || item.ClusterName == "" {
			return fmt.Errorf("%w: plan item missing cluster identity", framework.ErrInvalidReservationPlan)
		}
		if _, dup := seen[item.ClusterUID]; dup {
			return fmt.Errorf("%w: duplicated cluster %s in plan", framework.ErrInvalidReservationPlan, item.ClusterName)
		}
		seen[item.ClusterUID] = struct{}{}
	}
	return nil
}

// tryReserveLocked validates the whole plan and only mutates accounts when every leg is admissible.
func (l *pendingPlacementLedger) tryReserveLocked(plan framework.ReservationPlan) error {
	if l.staleCountLocked() >= l.maxStaleAccounts() {
		return framework.ErrLedgerStaleLimit
	}

	stagedItems := make([]*reservation, 0, len(plan.Items))
	totalAccounts := len(l.accounts)
	clusterAccountDelta := make(map[types.UID]int)
	fedAccountDelta := make(map[types.UID]int)

	for _, item := range plan.Items {
		snapshot, ok := l.snapshots[item.ClusterUID]
		if !ok {
			// No capacity basis known: fail closed rather than optimistically admitting.
			return fmt.Errorf("%w: cluster %s has no observed capacity snapshot", framework.ErrInsufficientReservedCapacity, item.ClusterName)
		}

		key := framework.LedgerKey{
			FederatedUID:        plan.FederatedUID,
			FederatedGeneration: plan.FederatedGeneration,
			ClusterUID:          item.ClusterUID,
			Attempt:             plan.Attempt,
		}
		_, exists := l.accounts[key]
		if !exists {
			totalAccounts++
			clusterAccountDelta[item.ClusterUID]++
			fedAccountDelta[plan.FederatedUID]++
		}

		// Effective demand = member-observed demand + in-flight reservations (excluding this exact key,
		// whose vector is being replaced) + the newly requested vector.
		memberDemand := memberObservedDemand(snapshot)
		reserved := l.sumResourcesLocked(item.ClusterUID, &key)
		if !resourceFits(snapshot.Allocatable, memberDemand, reserved, item.Resources) {
			return fmt.Errorf("%w: cluster %s cannot cover the requested vector", framework.ErrInsufficientReservedCapacity, item.ClusterName)
		}

		stagedItems = append(stagedItems, &reservation{
			key:         key,
			clusterName: item.ClusterName,
			resources:   item.Resources,
			createdAt:   l.now(),
			basisRV:     item.ClusterStatusResourceVersion,
		})
	}

	if err := l.checkCapsLocked(totalAccounts); err != nil {
		return err
	}
	for clusterUID, delta := range clusterAccountDelta {
		if l.clusterAccountCount(clusterUID)+delta > l.config.MaxAccountsPerCluster {
			return fmt.Errorf("%w: cluster %s", framework.ErrLedgerAccountLimit, l.clusterNames[clusterUID])
		}
	}
	for fedUID, delta := range fedAccountDelta {
		if l.fedAccountCount(fedUID)+delta > l.config.MaxAccountsPerFederatedObject {
			return fmt.Errorf("%w: federated object %s", framework.ErrLedgerAccountLimit, fedUID)
		}
	}

	// All legs passed: commit atomically.
	for _, entry := range stagedItems {
		l.putLocked(entry)
	}
	return nil
}

func (l *pendingPlacementLedger) checkCapsLocked(total int) error {
	if total > l.config.MaxAccounts {
		return framework.ErrLedgerAccountLimit
	}
	return nil
}

func (l *pendingPlacementLedger) putLocked(entry *reservation) {
	if _, exists := l.accounts[entry.key]; !exists {
		if _, ok := l.clusterIndex[entry.key.ClusterUID]; !ok {
			l.clusterIndex[entry.key.ClusterUID] = make(map[framework.LedgerKey]struct{})
		}
		l.clusterIndex[entry.key.ClusterUID][entry.key] = struct{}{}
		if _, ok := l.fedIndex[entry.key.FederatedUID]; !ok {
			l.fedIndex[entry.key.FederatedUID] = make(map[framework.LedgerKey]struct{})
		}
		l.fedIndex[entry.key.FederatedUID][entry.key] = struct{}{}
	}
	l.accounts[entry.key] = entry
	if entry.clusterName != "" {
		l.clusterNames[entry.key.ClusterUID] = entry.clusterName
	}
}

func (l *pendingPlacementLedger) removeLocked(key framework.LedgerKey) bool {
	if _, ok := l.accounts[key]; !ok {
		return false
	}
	delete(l.accounts, key)
	if set, ok := l.clusterIndex[key.ClusterUID]; ok {
		delete(set, key)
		if len(set) == 0 {
			delete(l.clusterIndex, key.ClusterUID)
		}
	}
	if set, ok := l.fedIndex[key.FederatedUID]; ok {
		delete(set, key)
		if len(set) == 0 {
			delete(l.fedIndex, key.FederatedUID)
		}
	}
	return true
}

func (l *pendingPlacementLedger) ReleaseAttempt(key framework.LedgerKey) int {
	l.mu.Lock()
	defer l.mu.Unlock()

	released := 0
	if key.ClusterUID != "" {
		if l.removeLocked(key) {
			released++
		}
	} else {
		for existing := range l.accounts {
			if existing.FederatedUID == key.FederatedUID &&
				existing.FederatedGeneration == key.FederatedGeneration &&
				existing.Attempt == key.Attempt {
				if l.removeLocked(existing) {
					released++
				}
			}
		}
	}
	if released > 0 {
		l.signalHeadLocked()
	}
	return released
}

func (l *pendingPlacementLedger) ReleasePriorAttempts(uid types.UID, generation int64, currentAttempt int64) int {
	l.mu.Lock()
	defer l.mu.Unlock()

	keys, ok := l.fedIndex[uid]
	if !ok {
		return 0
	}
	released := 0
	for key := range keys {
		if key.FederatedGeneration == generation && key.Attempt < currentAttempt {
			if l.removeLocked(key) {
				released++
			}
		}
	}
	if released > 0 {
		l.signalHeadLocked()
	}
	return released
}

func (l *pendingPlacementLedger) Confirm(key framework.LedgerKey) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.accounts[key]; !ok {
		// An exact key match is required: a stale confirmation can never release a newer attempt.
		return false, nil
	}
	l.removeLocked(key)
	l.signalHeadLocked()
	return true, nil
}

func (l *pendingPlacementLedger) ReleaseFederatedObject(uid types.UID) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	keys, ok := l.fedIndex[uid]
	if !ok {
		return 0
	}
	released := 0
	for key := range keys {
		if l.removeLocked(key) {
			released++
		}
	}
	if released > 0 {
		l.signalHeadLocked()
	}
	return released
}

func (l *pendingPlacementLedger) ReservedResources(clusterUID types.UID, exclude *framework.LedgerKey) framework.Resource {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.sumResourcesLocked(clusterUID, exclude)
}

func (l *pendingPlacementLedger) sumResourcesLocked(clusterUID types.UID, exclude *framework.LedgerKey) framework.Resource {
	total := framework.Resource{}
	for key := range l.clusterIndex[clusterUID] {
		if exclude != nil && key == *exclude {
			continue
		}
		entry := l.accounts[key]
		addResource(&total, &entry.resources)
	}
	return total
}

func (l *pendingPlacementLedger) Reconcile(input framework.ReconcileInput) framework.ReconcileResult {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, cluster := range input.Clusters {
		l.snapshots[cluster.ClusterUID] = cluster
		if cluster.ClusterName != "" {
			l.clusterNames[cluster.ClusterUID] = cluster.ClusterName
		}
	}

	result := framework.ReconcileResult{}

	// A non-nil cluster list is treated as an authoritative snapshot of the joined clusters: clusters
	// absent from it have left and their accounts can no longer protect any capacity.
	authoritativeClusters := input.Clusters != nil
	suppliedClusters := make(map[types.UID]struct{}, len(input.Clusters))
	for _, cluster := range input.Clusters {
		suppliedClusters[cluster.ClusterUID] = struct{}{}
	}

	// Only settle accounts belonging to observed objects; everything else stays untouched.
	for _, obj := range input.Objects {
		keys, ok := l.fedIndex[obj.UID]
		if !ok {
			continue
		}
		for key := range keys {
			entry, exists := l.accounts[key]
			if !exists {
				continue
			}
			cluster, clusterOK := l.snapshots[key.ClusterUID]
			_, clusterSupplied := suppliedClusters[key.ClusterUID]
			clusterGone := !clusterOK || (authoritativeClusters && !clusterSupplied)
			if clusterGone {
				// Cluster is gone: its capacity no longer exists, reclaim the account.
				l.removeLocked(key)
				result.Released = append(result.Released, key)
				continue
			}

			if obj.Generation > key.FederatedGeneration {
				// The generation owning this attempt was replaced. Only this exact attempt's account is
				// released; every other attempt remains untouched.
				l.removeLocked(key)
				result.Released = append(result.Released, key)
				continue
			}

			observed := findObjectClusterObservation(obj, entry.clusterName)
			if observed != nil && inclusionProvable(obj, *observed, entry.basisRV, cluster.ResourceVersion) {
				l.removeLocked(key)
				result.Confirmed = append(result.Confirmed, key)
				continue
			}

			// Inclusion cannot be proven with explicit evidence: conservatively retain.
			result.Retained = append(result.Retained, key)
		}
	}
	if len(result.Confirmed) > 0 || len(result.Released) > 0 {
		l.signalHeadLocked()
	}
	return result
}

func (l *pendingPlacementLedger) SweepExpired(now time.Time, input framework.ReconcileInput) framework.SweepResult {
	// Reuse the evidence-based reconcile path first so provably releasable stale accounts are reclaimed.
	reconcileResult := l.Reconcile(input)

	l.mu.Lock()
	defer l.mu.Unlock()

	result := framework.SweepResult{Released: len(reconcileResult.Confirmed) + len(reconcileResult.Released)}
	for _, entry := range l.accounts {
		if now.Sub(entry.createdAt) <= l.config.TTL {
			continue
		}
		entry.stale = true
		// TTL alone never releases; unprovable accounts keep consuming capacity.
		result.Stale++
	}
	return result
}

func (l *pendingPlacementLedger) Stats() framework.LedgerStats {
	l.mu.Lock()
	defer l.mu.Unlock()

	perCluster := make(map[string]int, len(l.clusterIndex))
	for uid, set := range l.clusterIndex {
		perCluster[l.clusterNames[uid]] = len(set)
	}
	perFed := make(map[string]int, len(l.fedIndex))
	for uid, set := range l.fedIndex {
		perFed[string(uid)] = len(set)
	}
	return framework.LedgerStats{
		Epoch:                l.epoch,
		Active:               l.active,
		Rebuilding:           l.rebuilding,
		TotalAccounts:        len(l.accounts),
		StaleAccounts:        l.staleCountLocked(),
		PerClusterAccounts:   perCluster,
		PerFederatedAccounts: perFed,
		Waiters:              len(l.waitQueue),
	}
}

func (l *pendingPlacementLedger) staleCountLocked() int {
	count := 0
	for _, entry := range l.accounts {
		if entry.stale {
			count++
		}
	}
	return count
}

func (l *pendingPlacementLedger) maxStaleAccounts() int {
	bound := l.config.MaxAccounts / staleAccountRatio
	if bound < 1 {
		return 1
	}
	return bound
}

func (l *pendingPlacementLedger) clusterAccountCount(clusterUID types.UID) int {
	return len(l.clusterIndex[clusterUID])
}

func (l *pendingPlacementLedger) fedAccountCount(uid types.UID) int {
	return len(l.fedIndex[uid])
}

func (l *pendingPlacementLedger) headTicketLocked() uint64 {
	if len(l.waitQueue) == 0 {
		return l.nextTicket
	}
	return l.waitQueue[0].ticket
}

func (l *pendingPlacementLedger) removeWaiter(w *waiter) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for i, candidate := range l.waitQueue {
		if candidate.ticket == w.ticket {
			l.waitQueue = append(l.waitQueue[:i], l.waitQueue[i+1:]...)
			break
		}
	}
	l.signalHeadLocked()
}

func (l *pendingPlacementLedger) signalHeadAfterUnlock() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.signalHeadLocked()
}

// signalHeadLocked wakes the queue head if it is waiting. Callers must hold l.mu.
func (l *pendingPlacementLedger) signalHeadLocked() {
	if len(l.waitQueue) == 0 {
		return
	}
	head := l.waitQueue[0]
	select {
	case head.ch <- struct{}{}:
	default:
	}
}

// inclusionProvable is the explicit, conservative confirmation predicate. Member inclusion of a reserved
// demand is only assumed when ALL hold:
//  1. the per-cluster propagation status is OK for the reservation's generation, and
//  2. the FederatedCluster status ResourceVersion has advanced past the reservation basis RV, proving a
//     capacity refresh happened after the reservation was committed.
//
// Arbitrary timestamp updates are never accepted as evidence, and unparseable RVs fail closed.
func inclusionProvable(
	obj framework.ObjectObservation,
	observed framework.ObjectClusterObservation,
	basisRV, currentRV string,
) bool {
	if obj.Generation <= 0 || observed.LastObservedGeneration < obj.Generation {
		return false
	}
	if observed.PropagationStatus != fedcorev1a1.ClusterPropagationOK {
		return false
	}
	basis, errBasis := strconv.ParseUint(basisRV, 10, 64)
	current, errCurrent := strconv.ParseUint(currentRV, 10, 64)
	if errBasis != nil || errCurrent != nil {
		return false
	}
	return current > basis
}

func findObjectClusterObservation(obj framework.ObjectObservation, clusterName string) *framework.ObjectClusterObservation {
	for i := range obj.Clusters {
		if obj.Clusters[i].ClusterName == clusterName {
			return &obj.Clusters[i]
		}
	}
	return nil
}

// memberObservedDemand derives the demand already counted by the member cluster snapshot:
// allocatable - available.
func memberObservedDemand(snapshot framework.ClusterObservation) framework.Resource {
	demand := framework.NewResource(snapshot.Allocatable)
	if len(snapshot.Available) == 0 {
		// No available basis reported: conservatively treat the whole allocatable as demanded.
		return *framework.NewResource(snapshot.Allocatable)
	}
	if err := demand.Sub(snapshot.Available); err != nil {
		// Fall back to the conservative interpretation instead of panicking on malformed snapshots.
		return *framework.NewResource(snapshot.Allocatable)
	}
	return *demand
}

// resourceFits checks whether allocatable covers member demand + reservations + the new request on every
// dimension. Values use the exact integer Quantity representation of the framework.
func resourceFits(allocatableList corev1.ResourceList, memberDemand, reserved framework.Resource, request framework.Resource) bool {
	allocatable := framework.NewResource(allocatableList)
	if allocatable.MilliCPU < memberDemand.MilliCPU+reserved.MilliCPU+request.MilliCPU {
		return false
	}
	if allocatable.Memory < memberDemand.Memory+reserved.Memory+request.Memory {
		return false
	}
	if allocatable.EphemeralStorage < memberDemand.EphemeralStorage+reserved.EphemeralStorage+request.EphemeralStorage {
		return false
	}
	names := make(
		map[corev1.ResourceName]struct{},
		len(allocatable.ScalarResources)+len(request.ScalarResources)+len(reserved.ScalarResources),
	)
	for name := range allocatable.ScalarResources {
		names[name] = struct{}{}
	}
	for name := range request.ScalarResources {
		names[name] = struct{}{}
	}
	for name := range reserved.ScalarResources {
		names[name] = struct{}{}
	}
	for name := range names {
		capacity := allocatable.ScalarResources[name]
		used := memberDemand.ScalarResources[name] + reserved.ScalarResources[name] + request.ScalarResources[name]
		if capacity < used {
			return false
		}
	}
	return true
}

func addResource(total *framework.Resource, delta *framework.Resource) {
	total.MilliCPU += delta.MilliCPU
	total.Memory += delta.Memory
	total.EphemeralStorage += delta.EphemeralStorage
	if len(delta.ScalarResources) == 0 {
		return
	}
	if total.ScalarResources == nil {
		total.ScalarResources = make(map[corev1.ResourceName]int64, len(delta.ScalarResources))
	}
	for name, q := range delta.ScalarResources {
		total.ScalarResources[name] += q
	}
}
