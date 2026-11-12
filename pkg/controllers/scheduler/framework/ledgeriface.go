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

package framework

import (
	"context"
	"errors"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	fedcorev1a1 "github.com/kubewharf/kubeadmiral/pkg/apis/core/v1alpha1"
)

// RebuildAttempt is the attempt number assigned to reservations reconstructed conservatively during a
// leader (re)start. Regular scheduling attempts always start from 1, so rebuilt reservations never collide
// with freshly committed attempts.
const RebuildAttempt int64 = 0

// Sentinel errors returned by the pending placement ledger. Callers should use errors.Is to inspect them.
var (
	// ErrLedgerRebuilding means the ledger is reconstructing its accounts after a leader (re)start and is
	// temporarily failing closed. Callers must retry the admission later instead of optimistically scheduling.
	ErrLedgerRebuilding = errors.New("pending placement ledger is rebuilding")
	// ErrLedgerInactive means the ledger's leader epoch has ended (e.g. leadership was lost) and no new
	// placement may be committed under the stale epoch.
	ErrLedgerInactive = errors.New("pending placement ledger epoch is inactive")
	// ErrInsufficientReservedCapacity means at least one cluster in the plan cannot cover the requested
	// resource vector on top of its member-observed demand and the existing in-flight reservations.
	ErrInsufficientReservedCapacity = errors.New("insufficient reservable cluster capacity")
	// ErrLedgerAccountLimit means a bounded account cap (global, per-cluster or per-object) would be exceeded.
	ErrLedgerAccountLimit = errors.New("pending placement ledger account limit exceeded")
	// ErrLedgerStaleLimit means too many reservations have outlived their TTL without a provable member
	// confirmation; admission is paused (fail-closed) until the stale accounts can be reconciled.
	ErrLedgerStaleLimit = errors.New("pending placement ledger stale account limit exceeded")
	// ErrTooManyWaiters means the bounded fair-waiting queue is full.
	ErrTooManyWaiters = errors.New("pending placement ledger waiter limit exceeded")
	// ErrUnknownReservation means no reservation matches the supplied exact ledger key.
	ErrUnknownReservation = errors.New("pending placement reservation not found")
	// ErrInvalidReservationPlan means the reservation plan is empty or contains malformed items.
	ErrInvalidReservationPlan = errors.New("invalid pending placement reservation plan")
)

// LedgerKey uniquely identifies a single in-flight reservation attempt. A confirmation or release only
// ever affects an exact key match, so a late confirmation of an old attempt can never release a newer
// attempt's reservation.
type LedgerKey struct {
	// FederatedUID is the UID of the FederatedObject / ClusterFederatedObject.
	FederatedUID types.UID
	// FederatedGeneration is the metadata.generation of the FederatedObject at scheduling time.
	FederatedGeneration int64
	// ClusterUID is the UID of the FederatedCluster the vector is reserved on.
	ClusterUID types.UID
	// Attempt distinguishes repeated scheduling attempts of the same federated object generation.
	Attempt int64
}

// ReservationPlanItem describes one cluster leg of a multi-cluster reservation plan.
type ReservationPlanItem struct {
	ClusterName string
	ClusterUID  types.UID
	// Resources is the exact resource vector to reserve on the cluster (already scaled by replica count
	// when applicable).
	Resources Resource
	// ClusterStatusResourceVersion is the ResourceVersion of the FederatedCluster object whose status
	// resources (allocatable/available snapshot) the admission decision was based on. It is the explicit
	// snapshot basis used when proving member inclusion later; timestamps alone are never trusted.
	ClusterStatusResourceVersion string
}

// ReservationPlan is an all-or-nothing multi-cluster reservation request. Either every item is reserved
// atomically or no account is mutated.
type ReservationPlan struct {
	FederatedUID        types.UID
	FederatedGeneration int64
	Attempt             int64
	Items               []ReservationPlanItem
}

// ClusterObservation is the currently observed capacity snapshot of a FederatedCluster.
type ClusterObservation struct {
	ClusterName     string
	ClusterUID      types.UID
	ResourceVersion string
	Allocatable     corev1.ResourceList
	Available       corev1.ResourceList
}

// ObjectClusterObservation is the per-cluster propagation observation of a FederatedObject.
type ObjectClusterObservation struct {
	ClusterName            string
	PropagationStatus      fedcorev1a1.PropagationStatusType
	LastObservedGeneration int64
}

// ObjectObservation is the currently observed scheduling state of a FederatedObject.
type ObjectObservation struct {
	UID        types.UID
	Generation int64
	Clusters   []ObjectClusterObservation
}

// ReconcileInput is a bounded snapshot of the informer caches used to reconcile or rebuild the ledger.
type ReconcileInput struct {
	Clusters []ClusterObservation
	Objects  []ObjectObservation
}

// ReconcileResult reports how each touched reservation was settled.
type ReconcileResult struct {
	Confirmed []LedgerKey
	Released  []LedgerKey
	Retained  []LedgerKey
}

// SweepResult reports the outcome of a TTL sweep.
type SweepResult struct {
	// Stale is the number of reservations past their TTL that are still retained because member inclusion
	// cannot be proven.
	Stale int
	// Released is the number of stale reservations whose release became provable and were reclaimed.
	Released int
}

// LedgerStats exposes bounded accounting information for observability and tests.
type LedgerStats struct {
	Epoch                uint64
	Active               bool
	Rebuilding           bool
	TotalAccounts        int
	StaleAccounts        int
	PerClusterAccounts   map[string]int
	PerFederatedAccounts map[string]int
	Waiters              int
}

// LedgerConfig configures the optional PendingPlacementLedger. All bounds must be finite; zero values are
// normalized to defaults.
type LedgerConfig struct {
	Enabled bool
	// TTL is the maximum lifetime a reservation may remain unconfirmed before being flagged stale.
	// TTL expiry alone never releases an account while member inclusion is unprovable; it only bounds
	// staleness and eventually pauses admission.
	TTL time.Duration
	// MaxAccounts bounds the total number of reservation accounts held by the ledger.
	MaxAccounts int
	// MaxAccountsPerCluster bounds reservation accounts on a single FederatedCluster.
	MaxAccountsPerCluster int
	// MaxAccountsPerFederatedObject bounds reservation accounts belonging to a single FederatedObject.
	MaxAccountsPerFederatedObject int
	// MaxWaiters bounds the number of concurrent fair-waiting admission requests.
	MaxWaiters int
	// ReserveTimeout bounds how long a Reserve call may wait for capacity in the fair-waiting queue.
	ReserveTimeout time.Duration
	// RebuildTimeout bounds the bounded-rebuild phase after a leader (re)start.
	RebuildTimeout time.Duration
}

// DefaultLedgerConfig returns a conservative set of finite bounds.
func DefaultLedgerConfig() LedgerConfig {
	return LedgerConfig{
		Enabled:                       false,
		TTL:                           10 * time.Minute,
		MaxAccounts:                   10000,
		MaxAccountsPerCluster:         1000,
		MaxAccountsPerFederatedObject: 256,
		MaxWaiters:                    128,
		ReserveTimeout:                5 * time.Second,
		RebuildTimeout:                30 * time.Second,
	}
}

// Normalize fills unset fields with defaults and clamps invalid values.
func (c *LedgerConfig) Normalize() {
	defaults := DefaultLedgerConfig()
	if c.TTL <= 0 {
		c.TTL = defaults.TTL
	}
	if c.MaxAccounts <= 0 {
		c.MaxAccounts = defaults.MaxAccounts
	}
	if c.MaxAccountsPerCluster <= 0 {
		c.MaxAccountsPerCluster = defaults.MaxAccountsPerCluster
	}
	if c.MaxAccountsPerFederatedObject <= 0 {
		c.MaxAccountsPerFederatedObject = defaults.MaxAccountsPerFederatedObject
	}
	if c.MaxWaiters <= 0 {
		c.MaxWaiters = defaults.MaxWaiters
	}
	if c.ReserveTimeout <= 0 {
		c.ReserveTimeout = defaults.ReserveTimeout
	}
	if c.RebuildTimeout <= 0 {
		c.RebuildTimeout = defaults.RebuildTimeout
	}
	if c.MaxAccountsPerCluster > c.MaxAccounts {
		c.MaxAccountsPerCluster = c.MaxAccounts
	}
	if c.MaxAccountsPerFederatedObject > c.MaxAccounts {
		c.MaxAccountsPerFederatedObject = c.MaxAccounts
	}
}

// PendingPlacementLedger reserves cluster resource vectors for placement demands that this scheduler
// leader has committed but which have not yet been observably accounted for by the member clusters.
//
// It only constrains in-flight federated demands of the owning leader; it never claims to govern other
// schedulers acting locally on a member cluster. All implementations must be safe for concurrent use.
type PendingPlacementLedger interface {
	// Epoch returns the leader epoch of this ledger instance. Reservations are always scoped to an epoch;
	// an old epoch can never commit a new placement.
	Epoch() uint64

	// Rebuild conservatively reconstructs reservations from informer state after a leader (re)start.
	// Placements whose member inclusion cannot be proven are re-reserved under the new epoch; provably
	// observed placements are not. Admission fails closed while rebuilding and the rebuild itself is
	// bounded by ReconcileInput size, RebuildTimeout and the account caps.
	Rebuild(ctx context.Context, input ReconcileInput) error

	// Reserve atomically reserves every item of the plan or fails without mutating any account. A reserve
	// for an already existing exact key replaces its vector instead of double charging. It may wait fairly
	// (FIFO) up to ReserveTimeout / ctx deadline, bounded by MaxWaiters.
	Reserve(ctx context.Context, plan ReservationPlan) error

	// ReleaseAttempt releases every reservation belonging to the exact attempt identified by key (same
	// federated UID, generation and attempt), optionally restricted to one cluster when key.ClusterUID is
	// set. Reservations of other attempts are never touched.
	ReleaseAttempt(key LedgerKey) int

	// ReleasePriorAttempts releases every reservation of a federated object whose attempt number is
	// strictly smaller than currentAttempt within the given generation. It is used when a newer plan
	// supersedes older attempts of the same generation; other federated objects are never affected.
	ReleasePriorAttempts(uid types.UID, generation int64, currentAttempt int64) int

	// Confirm releases a reservation only when the exact key matches. A stale confirmation carrying an old
	// generation or attempt cannot release a newer attempt. It reports whether an account was released.
	Confirm(key LedgerKey) (bool, error)

	// Reconcile settles reservations against fresh observations using explicit confirmation evidence:
	// member inclusion is only assumed when the per-cluster propagation status is OK at the reservation
	// generation and the cluster status snapshot has advanced past the reservation's basis RV. When the
	// inclusion cannot be proven, accounts are conservatively retained.
	Reconcile(input ReconcileInput) ReconcileResult

	// ReservedResources returns the sum of reserved vectors on the cluster, optionally excluding one exact
	// key (typically the caller's own in-flight attempt, to avoid self double charging).
	ReservedResources(clusterUID types.UID, exclude *LedgerKey) Resource

	// ReleaseFederatedObject reclaims every reservation of a federated object, e.g. on deletion.
	ReleaseFederatedObject(uid types.UID) int

	// SweepExpired flags TTL-expired reservations as stale and reclaims those whose release is now
	// provable. Stale accounts keep consuming capacity; when the stale cap is hit, admission pauses.
	SweepExpired(now time.Time, input ReconcileInput) SweepResult

	// Stats returns a point-in-time, bounded snapshot of the ledger accounts.
	Stats() LedgerStats

	// Deactivate ends the ledger epoch, e.g. when leadership is lost or the scheduler is shutting down.
	// After deactivation every new commit fails with ErrLedgerInactive.
	Deactivate()
}
