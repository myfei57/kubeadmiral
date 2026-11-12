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

package statuscoverage

import (
	"testing"

	fedcorev1a1 "github.com/kubewharf/kubeadmiral/pkg/apis/core/v1alpha1"
)

func TestAggregate_CompleteEnvelopeProducesPreciseTotals(t *testing.T) {
	fedObject := newFedObject(3, 3, []string{"c1", "c2"}, map[string]int64{"c1": 3, "c2": 3})
	envelope := Build(fedObject, []MemberObservation{
		readyObservation("c1", newMemberObject("uid-c1", "1", 3, map[string]interface{}{
			"replicas":      int64(3),
			"readyReplicas": int64(2),
		})),
		readyObservation("c2", newMemberObject("uid-c2", "2", 3, map[string]interface{}{
			"replicas":      int64(0),
			"readyReplicas": int64(0),
		})),
	}, BuildOptions{
		Strategy:   fedcorev1a1.CoverageStrategyStrictAll,
		Fields:     []string{testFieldReplicas, testFieldReadyReplicas},
		SourceKind: "Deployment",
	})

	aggregated, err := Aggregate(envelope)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !aggregated.IsComplete() {
		t.Fatalf("complete strict envelope must aggregate to a complete result")
	}

	replicas := aggregated.FindField(testFieldReplicas)
	if replicas == nil || replicas.ObservedValue != 3 || replicas.LowerBound != 3 {
		t.Fatalf("expected precise replica total 3 (including a real 0), got %+v", replicas)
	}
	if replicas.UpperBound == nil || *replicas.UpperBound != 3 {
		t.Fatalf("complete field must carry a finite upper bound equal to the total, got %+v", replicas)
	}
	if replicas.MissingClusters != 0 || !replicas.Complete {
		t.Fatalf("complete field must have no missing clusters: %+v", replicas)
	}
}

func TestAggregate_PartialEnvelopeIsRangeAndNeverMistakenForComplete(t *testing.T) {
	fedObject := newFedObject(3, 3, []string{"c1", "c2"}, map[string]int64{"c1": 3, "c2": 3})
	envelope := Build(fedObject, []MemberObservation{
		readyObservation("c1", newMemberObject("uid-c1", "1", 3, map[string]interface{}{
			"replicas":      int64(4),
			"readyReplicas": int64(4),
		})),
		// c2 is unreachable: its replicas are unknown, never zero.
		{ClusterName: "c2", ClusterUID: "cluster-uid-c2", Ready: true, ObjectError: assertError("unreachable")},
	}, BuildOptions{
		Strategy:   fedcorev1a1.CoverageStrategyPartial,
		Fields:     []string{testFieldReplicas, testFieldReadyReplicas},
		SourceKind: "Deployment",
	})

	aggregated, err := Aggregate(envelope)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// The caller-facing guard must unambiguously reject this as complete fleet health.
	if aggregated.IsComplete() || aggregated.Complete {
		t.Fatalf("a partial envelope must never be reported as complete")
	}

	replicas := aggregated.FindField(testFieldReplicas)
	if replicas == nil {
		t.Fatalf("missing replicas aggregate")
	}
	// Only c1's observed 4 replicas are summed; the missing cluster is not converted to zero.
	if replicas.ObservedValue != 4 || replicas.LowerBound != 4 {
		t.Fatalf("partial total must be the observed lower bound 4, got %+v", replicas)
	}
	if replicas.UpperBound != nil {
		t.Fatalf("partial totals must be unbounded ranges, not fabricated precise values: %+v", replicas)
	}
	if replicas.MissingClusters != 1 || replicas.PresentClusters != 1 {
		t.Fatalf("expected one missing and one present cluster, got %+v", replicas)
	}
}

func TestAggregate_StrictIncompleteAlsoRefusesPreciseTotals(t *testing.T) {
	fedObject := newFedObject(3, 3, []string{"c1", "c2"}, map[string]int64{"c1": 3, "c2": 3})
	envelope := Build(fedObject, []MemberObservation{
		readyObservation("c1", newMemberObject("uid-c1", "1", 3, map[string]interface{}{"replicas": int64(2)})),
		{ClusterName: "c2", ClusterUID: "cluster-uid-c2", Ready: false},
	}, BuildOptions{
		Strategy:   fedcorev1a1.CoverageStrategyStrictAll,
		Fields:     []string{testFieldReplicas},
		SourceKind: "Deployment",
	})

	aggregated, err := Aggregate(envelope)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if aggregated.IsComplete() {
		t.Fatalf("strict strategy must not hide missing observations behind a precise total")
	}
	replicas := aggregated.FindField(testFieldReplicas)
	if replicas.UpperBound != nil || replicas.ObservedValue != 2 {
		t.Fatalf("strict-incomplete aggregate must stay a lower-bound range, got %+v", replicas)
	}
}

func TestAggregate_NonNumericValueIsCountedNotSummed(t *testing.T) {
	fedObject := newFedObject(1, 1, []string{"c1"}, map[string]int64{"c1": 1})
	object := newMemberObject("uid-c1", "1", 1, map[string]interface{}{"state": "Running"})
	envelope := Build(fedObject, []MemberObservation{readyObservation("c1", object)}, BuildOptions{
		Strategy:   fedcorev1a1.CoverageStrategyStrictAll,
		Fields:     []string{"status.state"},
		SourceKind: "Deployment",
	})

	aggregated, err := Aggregate(envelope)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	field := aggregated.FindField("status.state")
	if field == nil || field.NonNumericValues != 1 || field.ObservedValue != 0 {
		t.Fatalf("non-numeric present value must be counted without contributing a sum, got %+v", field)
	}
}

func TestAggregate_NilEnvelopeErrors(t *testing.T) {
	if _, err := Aggregate(nil); err == nil {
		t.Fatalf("aggregating a nil envelope must return an error")
	}
}

type errString string

func (e errString) Error() string { return string(e) }

func assertError(message string) error { return errString(message) }
