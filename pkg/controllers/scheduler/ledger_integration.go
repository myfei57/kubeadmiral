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
	"errors"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"

	fedcorev1a1 "github.com/kubewharf/kubeadmiral/pkg/apis/core/v1alpha1"
	"github.com/kubewharf/kubeadmiral/pkg/controllers/common"
	"github.com/kubewharf/kubeadmiral/pkg/controllers/scheduler/core"
	"github.com/kubewharf/kubeadmiral/pkg/controllers/scheduler/framework"
	"github.com/kubewharf/kubeadmiral/pkg/controllers/scheduler/ledger"
	"github.com/kubewharf/kubeadmiral/pkg/util/worker"
)

// ledgerRetryDelay bounds how long an object waits before retrying an admission that the ledger could not
// satisfy immediately. It keeps the retry path finite and avoids hot-looping the workqueue.
const ledgerRetryDelay = 5 * time.Second

// rebuildRetryDelay bounds the bounded-rebuild retry loop after a leader (re)start.
const rebuildRetryDelay = time.Second

type attemptState struct {
	uid        types.UID
	generation int64
	attempt    int64
}

// nextAttempt mints a strictly increasing attempt number for one FederatedObject scheduling lifecycle.
// Attempts are the finest ledger scope: cancelling or replacing a plan only ever releases the owning
// attempt, and a stale confirmation can never touch a newer attempt.
func (s *Scheduler) nextAttempt(key string, uid types.UID, generation int64) int64 {
	s.attemptMu.Lock()
	defer s.attemptMu.Unlock()
	state := s.attempts[key]
	state.uid = uid
	state.generation = generation
	state.attempt++
	s.attempts[key] = state
	return state.attempt
}

func (s *Scheduler) trackFederatedUID(key string, uid types.UID) {
	s.attemptMu.Lock()
	defer s.attemptMu.Unlock()
	s.knownUIDs[key] = uid
}

func (s *Scheduler) forgetFederatedUID(key string) (types.UID, bool) {
	s.attemptMu.Lock()
	defer s.attemptMu.Unlock()
	uid, ok := s.knownUIDs[key]
	delete(s.knownUIDs, key)
	delete(s.attempts, key)
	return uid, ok
}

// reserveSchedulingResult atomically reserves the full multi-cluster plan derived from the scheduling
// result. A non-nil returned worker result means admission failed and the reconcile must follow it.
func (s *Scheduler) reserveSchedulingResult(
	ctx context.Context,
	fedObject fedcorev1a1.GenericFederatedObject,
	su *framework.SchedulingUnit,
	result core.ScheduleResult,
	clusters []*fedcorev1a1.FederatedCluster,
) *worker.Result {
	if s.ledger == nil {
		return nil
	}
	logger := klog.FromContext(ctx)

	if len(result.SuggestedClusters) == 0 {
		// An empty placement has nothing to reserve; any prior accounts of the object are settled
		// elsewhere (deschedule, generation replacement or evidence-based reconcile).
		return nil
	}

	plan, err := s.buildReservationPlan(su, result, clusters)
	if err != nil {
		logger.Error(err, "Failed to build pending placement reservation plan")
		return &worker.StatusError
	}

	if err := s.ledger.Reserve(ctx, plan); err != nil {
		switch {
		case errors.Is(err, framework.ErrLedgerInactive):
			// Leadership was lost; nothing new may be committed under the stale epoch.
			logger.Info("Pending placement ledger is inactive, skip committing placement")
			return &worker.StatusAllOK
		case errors.Is(err, framework.ErrLedgerRebuilding):
			logger.Info("Pending placement ledger is rebuilding, requeue placement")
			return &worker.Result{Success: true, RequeueAfter: durationPtr(ledgerRetryDelay)}
		case errors.Is(err, framework.ErrInsufficientReservedCapacity):
			logger.Info("Placement rejected by pending placement ledger, requeue with delay", "err", err.Error())
			s.eventRecorder.Eventf(
				fedObject,
				corev1.EventTypeWarning,
				EventReasonScheduleFederatedObject,
				"Pending placement ledger rejected reservation: %v",
				err,
			)
			return &worker.Result{Success: true, RequeueAfter: durationPtr(ledgerRetryDelay)}
		case errors.Is(err, framework.ErrLedgerAccountLimit),
			errors.Is(err, framework.ErrLedgerStaleLimit),
			errors.Is(err, framework.ErrTooManyWaiters),
			errors.Is(err, framework.ErrInvalidReservationPlan):
			logger.Error(err, "Pending placement ledger bounded failure")
			return &worker.StatusError
		default:
			logger.Error(err, "Failed to reserve pending placement")
			return &worker.StatusError
		}
	}
	return nil
}

func durationPtr(d time.Duration) *time.Duration {
	return &d
}

// buildReservationPlan converts a ScheduleResult into an all-or-nothing ledger plan. In Divide mode the
// per-replica resource request is scaled exactly by the assigned replica count; in Duplicate mode the
// full request is reserved on every selected cluster.
func (s *Scheduler) buildReservationPlan(
	su *framework.SchedulingUnit,
	result core.ScheduleResult,
	clusters []*fedcorev1a1.FederatedCluster,
) (framework.ReservationPlan, error) {
	plan := framework.ReservationPlan{
		FederatedUID:        su.FederatedUID,
		FederatedGeneration: su.FederatedGeneration,
		Attempt:             su.ScheduleAttempt,
		Items:               make([]framework.ReservationPlanItem, 0, len(result.SuggestedClusters)),
	}

	clusterByName := make(map[string]*fedcorev1a1.FederatedCluster, len(clusters))
	for _, cluster := range clusters {
		clusterByName[cluster.Name] = cluster
	}

	for clusterName, replicas := range result.SuggestedClusters {
		cluster, ok := clusterByName[clusterName]
		if !ok {
			return framework.ReservationPlan{}, fmt.Errorf("selected cluster %s is absent from the candidate list", clusterName)
		}

		vector := su.ResourceRequest
		if replicas != nil {
			scaled, err := ledger.ScaleResource(su.ResourceRequest, *replicas)
			if err != nil {
				return framework.ReservationPlan{}, fmt.Errorf("failed to scale reservation for cluster %s: %w", clusterName, err)
			}
			vector = scaled
		}

		plan.Items = append(plan.Items, framework.ReservationPlanItem{
			ClusterName:                  cluster.Name,
			ClusterUID:                   cluster.UID,
			Resources:                    vector,
			ClusterStatusResourceVersion: cluster.ResourceVersion,
		})
	}
	return plan, nil
}

// buildObjectObservation extracts the explicit per-cluster confirmation evidence carried by a federated
// object. Only OK statuses with a LastObservedGeneration at least at the reservation generation can ever
// prove member inclusion; timestamps are deliberately not used.
func buildObjectObservation(obj fedcorev1a1.GenericFederatedObject) framework.ObjectObservation {
	observed := framework.ObjectObservation{
		UID:        obj.GetUID(),
		Generation: obj.GetGeneration(),
		Clusters:   make([]framework.ObjectClusterObservation, 0),
	}
	status := obj.GetStatus()
	if status == nil {
		return observed
	}
	for _, clusterStatus := range status.Clusters {
		observed.Clusters = append(observed.Clusters, framework.ObjectClusterObservation{
			ClusterName:            clusterStatus.Cluster,
			PropagationStatus:      clusterStatus.Status,
			LastObservedGeneration: clusterStatus.LastObservedGeneration,
		})
	}
	return observed
}

func buildClusterObservations(clusters []*fedcorev1a1.FederatedCluster) []framework.ClusterObservation {
	observations := make([]framework.ClusterObservation, 0, len(clusters))
	for _, cluster := range clusters {
		observations = append(observations, framework.ClusterObservation{
			ClusterName:     cluster.Name,
			ClusterUID:      cluster.UID,
			ResourceVersion: cluster.ResourceVersion,
			Allocatable:     cluster.Status.Resources.Allocatable,
			Available:       cluster.Status.Resources.Available,
		})
	}
	return observations
}

func (s *Scheduler) reconcileLedger(
	obj fedcorev1a1.GenericFederatedObject,
	clusters []*fedcorev1a1.FederatedCluster,
) framework.ReconcileResult {
	return s.ledger.Reconcile(framework.ReconcileInput{
		Clusters: buildClusterObservations(clusters),
		Objects:  []framework.ObjectObservation{buildObjectObservation(obj)},
	})
}

// buildFullLedgerInput assembles a bounded snapshot of all federated clusters and federated objects from
// the informer caches for rebuild and TTL sweeps.
func (s *Scheduler) buildFullLedgerInput() (framework.ReconcileInput, error) {
	allClusters, err := s.federatedClusterInformer.Lister().List(labels.Everything())
	if err != nil {
		return framework.ReconcileInput{}, fmt.Errorf("failed to list federated clusters: %w", err)
	}

	objects := make([]framework.ObjectObservation, 0)
	fedObjects, err := s.fedObjectInformer.Lister().List(labels.Everything())
	if err != nil {
		return framework.ReconcileInput{}, fmt.Errorf("failed to list federated objects: %w", err)
	}
	for _, obj := range fedObjects {
		objects = append(objects, buildObjectObservation(obj))
	}
	clusterFedObjects, err := s.clusterFedObjectInformer.Lister().List(labels.Everything())
	if err != nil {
		return framework.ReconcileInput{}, fmt.Errorf("failed to list cluster federated objects: %w", err)
	}
	for _, obj := range clusterFedObjects {
		objects = append(objects, buildObjectObservation(obj))
	}

	return framework.ReconcileInput{
		Clusters: buildClusterObservations(allClusters),
		Objects:  objects,
	}, nil
}

// rebuildLedger runs the bounded rebuild protocol after a leader (re)start. It keeps failing closed
// (retries) until reconstruction succeeds or the leader context ends; the ledger never admits new
// placements while rebuilding, and a fresh epoch guarantees stale leaders cannot commit.
func (s *Scheduler) rebuildLedger(ctx context.Context) error {
	logger := klog.FromContext(ctx)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		input, err := s.buildFullLedgerInput()
		if err != nil {
			logger.Error(err, "Failed to build ledger rebuild input")
			if !waitWithContext(ctx, rebuildRetryDelay) {
				return ctx.Err()
			}
			continue
		}
		if err := s.ledger.Rebuild(ctx, input); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			logger.Error(err, "Pending placement ledger rebuild failed, retrying fail-closed")
			if !waitWithContext(ctx, rebuildRetryDelay) {
				return ctx.Err()
			}
			continue
		}
		s.enqueueAllFederatedObjectsForLedger()
		return nil
	}
}

func (s *Scheduler) runLedgerSweeper(ctx context.Context) {
	logger := klog.FromContext(ctx)
	interval := s.ledgerConfig.TTL / 2
	if interval < time.Second {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			input, err := s.buildFullLedgerInput()
			if err != nil {
				logger.Error(err, "Failed to build ledger sweep input")
				continue
			}
			result := s.ledger.SweepExpired(time.Now(), input)
			if result.Stale > 0 {
				logger.Info("Pending placement ledger retains stale unconfirmed accounts",
					"stale", result.Stale, "released", result.Released)
			}
		}
	}
}

// enqueueAllFederatedObjectsForLedger triggers a reconcile of every federated object after a rebuild so
// that exact resource vectors are re-reserved under regular attempts without waiting for external events.
func (s *Scheduler) enqueueAllFederatedObjectsForLedger() {
	fedObjects, err := s.fedObjectInformer.Lister().List(labels.Everything())
	if err != nil {
		klog.Background().Error(err, "Failed to enqueue federated objects after ledger rebuild")
	} else {
		for _, obj := range fedObjects {
			s.worker.Enqueue(common.NewQualifiedName(obj))
		}
	}
	clusterFedObjects, err := s.clusterFedObjectInformer.Lister().List(labels.Everything())
	if err != nil {
		klog.Background().Error(err, "Failed to enqueue cluster federated objects after ledger rebuild")
		return
	}
	for _, obj := range clusterFedObjects {
		s.worker.Enqueue(common.NewQualifiedName(obj))
	}
}

func waitWithContext(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
