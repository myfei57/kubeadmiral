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

package clusterresources

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"

	fedcorev1a1 "github.com/kubewharf/kubeadmiral/pkg/apis/core/v1alpha1"
	"github.com/kubewharf/kubeadmiral/pkg/controllers/scheduler/framework"
	schedulerledger "github.com/kubewharf/kubeadmiral/pkg/controllers/scheduler/ledger"
)

type fakeLedgerHandle struct {
	ledger framework.PendingPlacementLedger
}

func (f *fakeLedgerHandle) DynamicClient() dynamic.Interface { return nil }

func (f *fakeLedgerHandle) PendingPlacementLedger() framework.PendingPlacementLedger { return f.ledger }

func makeLedgerCluster() *fedcorev1a1.FederatedCluster {
	return &fedcorev1a1.FederatedCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "cluster-ledger",
			UID:             types.UID("cluster-ledger-uid"),
			ResourceVersion: "100",
		},
		Status: fedcorev1a1.FederatedClusterStatus{
			Resources: fedcorev1a1.Resources{
				Allocatable: corev1.ResourceList{
					corev1.ResourceCPU: *resource.NewMilliQuantity(10000, resource.DecimalSI),
				},
				Available: corev1.ResourceList{
					corev1.ResourceCPU: *resource.NewMilliQuantity(10000, resource.DecimalSI),
				},
			},
		},
	}
}

func makeIdentitySU(name string, uid types.UID, generation, attempt int64, milliCPU int64) *framework.SchedulingUnit {
	return &framework.SchedulingUnit{
		Name:                name,
		FederatedUID:        uid,
		FederatedGeneration: generation,
		ScheduleAttempt:     attempt,
		ResourceRequest:     framework.Resource{MilliCPU: milliCPU},
	}
}

// TestFilterWithLedgerAccountsReservations verifies the Fit filter charges ledger reservations on top of
// the member-observed demand and excludes the scheduling unit's own in-flight attempt.
func TestFilterWithLedgerAccountsReservations(t *testing.T) {
	cluster := makeLedgerCluster()
	cfg := framework.DefaultLedgerConfig()
	cfg.Enabled = true
	ledger := schedulerledger.NewPendingPlacementLedger(cfg)
	if err := ledger.Rebuild(context.Background(), framework.ReconcileInput{
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

	plugin, err := NewClusterResourcesFit(&fakeLedgerHandle{ledger: ledger})
	if err != nil {
		t.Fatalf("failed to create plugin: %v", err)
	}

	// First workload reserves 7000 milli-cpu out of 10000.
	first := makeIdentitySU("su-1", types.UID("fed-1"), 1, 1, 7000)
	if err := ledger.Reserve(context.Background(), framework.ReservationPlan{
		FederatedUID:        first.FederatedUID,
		FederatedGeneration: first.FederatedGeneration,
		Attempt:             first.ScheduleAttempt,
		Items: []framework.ReservationPlanItem{{
			ClusterName:                  cluster.Name,
			ClusterUID:                   cluster.UID,
			Resources:                    framework.Resource{MilliCPU: 7000},
			ClusterStatusResourceVersion: cluster.ResourceVersion,
		}},
	}); err != nil {
		t.Fatalf("reserve failed: %v", err)
	}

	// A second workload requesting 5000 must be filtered out: 7000 reserved + 5000 requested > 10000.
	second := makeIdentitySU("su-2", types.UID("fed-2"), 1, 1, 5000)
	result := plugin.(framework.FilterPlugin).Filter(context.TODO(), second, cluster)
	if result.IsSuccess() {
		t.Fatalf("expected second workload to be unschedulable against ledger reservations, got success")
	}

	// A small workload that fits in the remaining 3000 passes.
	small := makeIdentitySU("su-3", types.UID("fed-3"), 1, 1, 3000)
	if result := plugin.(framework.FilterPlugin).Filter(context.TODO(), small, cluster); !result.IsSuccess() {
		t.Fatalf("expected small workload to fit, got %v", result.Message())
	}

	// The first workload's own reconcile excludes its reservation, so it is not double charged.
	if result := plugin.(framework.FilterPlugin).Filter(context.TODO(), first, cluster); !result.IsSuccess() {
		t.Fatalf("expected own attempt to be excluded from reservation accounting, got %v", result.Message())
	}

	// After confirmation the capacity returns and the second workload becomes admissible.
	ok, err := ledger.Confirm(framework.LedgerKey{
		FederatedUID:        first.FederatedUID,
		FederatedGeneration: first.FederatedGeneration,
		Attempt:             first.ScheduleAttempt,
		ClusterUID:          cluster.UID,
	})
	if err != nil || !ok {
		t.Fatalf("confirmation failed, ok=%v err=%v", ok, err)
	}
	if result := plugin.(framework.FilterPlugin).Filter(context.TODO(), second, cluster); !result.IsSuccess() {
		t.Fatalf("expected second workload to fit after reclaim, got %v", result.Message())
	}
}

// TestFilterWithoutLedgerUnchanged guarantees the original Fit behavior when no ledger is wired, even if
// the scheduling unit carries identity fields.
func TestFilterWithoutLedgerUnchanged(t *testing.T) {
	cluster := makeLedgerCluster()
	plugin, err := NewClusterResourcesFit(&fakeLedgerHandle{ledger: nil})
	if err != nil {
		t.Fatalf("failed to create plugin: %v", err)
	}

	su := makeIdentitySU("su", types.UID("fed"), 1, 1, 10000)
	result := plugin.(framework.FilterPlugin).Filter(context.TODO(), su, cluster)
	if !result.IsSuccess() {
		t.Fatalf("expected unchanged fit behavior without ledger, got %v", result.Message())
	}

	// A nil handle, as used by the historical constructor tests, stays safe.
	nilPlugin, _ := NewClusterResourcesFit(nil)
	if result := nilPlugin.(framework.FilterPlugin).Filter(context.TODO(), su, cluster); !result.IsSuccess() {
		t.Fatalf("expected nil handle to behave as before, got %v", result.Message())
	}
}
