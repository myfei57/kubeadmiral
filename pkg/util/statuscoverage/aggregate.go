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
	"encoding/json"
	"fmt"
	"sort"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"

	fedcorev1a1 "github.com/kubewharf/kubeadmiral/pkg/apis/core/v1alpha1"
)

// AggregatedFieldCoverage is the per-field aggregation result. Missing observations are never
// converted to zero: incomplete fields carry an explicit lower bound and an unbounded upper bound
// instead of a fabricated precise total.
type AggregatedFieldCoverage struct {
	// Path is the collected field path.
	Path string `json:"path"`
	// Complete is true only when every target cluster observed the field as Present and no
	// truncation occurred.
	Complete bool `json:"complete"`
	// PresentClusters is the number of clusters whose Present value was summed.
	PresentClusters int `json:"presentClusters"`
	// MissingClusters is the number of target clusters without a Present value for this field.
	MissingClusters int `json:"missingClusters"`
	// NonNumericValues counts Present values that could not be summed as integers.
	NonNumericValues int `json:"nonNumericValues,omitempty"`
	// ObservedValue is the sum of the observed Present values.
	ObservedValue int64 `json:"observedValue"`
	// LowerBound equals ObservedValue: every missing observation contributes at least zero.
	LowerBound int64 `json:"lowerBound"`
	// UpperBound is nil while the field is incomplete (the missing replicas are unbounded) and is
	// set to a precise total only when the field is Complete.
	UpperBound *int64 `json:"upperBound,omitempty"`
}

// AggregatedCoverage is the summary handed to status aggregators. Callers must gate on IsComplete
// before treating any numeric total as a precise whole-fleet value.
type AggregatedCoverage struct {
	// Strategy echoes the envelope strategy.
	Strategy fedcorev1a1.CoverageStrategy `json:"strategy"`
	// Complete is true only for a non-stale StrictAll envelope whose every target cluster observed
	// every configured field at the current propagation generation.
	Complete bool `json:"complete"`
	// Stale mirrors the envelope staleness flag.
	Stale bool `json:"stale,omitempty"`
	// TargetClusters is the size of the explicit target cluster set.
	TargetClusters int `json:"targetClusters"`
	// ObservedClusters is the number of clusters with a current-generation object receipt.
	ObservedClusters int `json:"observedClusters"`
	// Truncated mirrors the envelope truncation flag.
	Truncated bool `json:"truncated,omitempty"`
	// Fields holds the aggregated result for every collected field path.
	Fields []AggregatedFieldCoverage `json:"fields"`
}

// IsComplete is the caller-facing guard: a false result means numeric totals are partial ranges and
// must never be interpreted as complete fleet health.
func (a *AggregatedCoverage) IsComplete() bool {
	if a == nil || a.Stale || a.Truncated || !a.Complete {
		return false
	}
	for _, field := range a.Fields {
		if !field.Complete {
			return false
		}
	}
	return true
}

// Aggregate reduces a coverage envelope into per-field totals. It never invents zeroes for missing
// observations: under Partial (or an incomplete StrictAll envelope) totals are published as
// [lower bound, +inf) ranges, and precise totals are produced only when every target cluster is
// observed.
func Aggregate(coverage *fedcorev1a1.CollectedStatusCoverage) (*AggregatedCoverage, error) {
	if coverage == nil {
		return nil, fmt.Errorf("cannot aggregate a nil coverage envelope")
	}

	paths := make([]string, 0)
	seen := make(map[string]struct{})
	for _, cluster := range coverage.Clusters {
		for _, field := range cluster.Fields {
			if _, ok := seen[field.Path]; ok {
				continue
			}
			seen[field.Path] = struct{}{}
			paths = append(paths, field.Path)
		}
	}
	sort.Strings(paths)

	aggregated := &AggregatedCoverage{
		Strategy:         coverage.Strategy,
		Complete:         coverage.Complete,
		Stale:            coverage.Stale,
		TargetClusters:   coverage.TargetClusters,
		ObservedClusters: coverage.ObservedClusters,
		Truncated:        coverage.Truncated,
		Fields:           make([]AggregatedFieldCoverage, 0, len(paths)),
	}

	for _, path := range paths {
		fieldAggregate := AggregatedFieldCoverage{Path: path}
		for _, cluster := range coverage.Clusters {
			field := findField(cluster.Fields, path)
			if field == nil || field.State != fedcorev1a1.CoveragePresent || field.Value == nil {
				continue
			}
			value, ok, err := integerValue(field.Value)
			if err != nil {
				return nil, fmt.Errorf("field %q in cluster %q: %w", path, cluster.Cluster, err)
			}
			if !ok {
				fieldAggregate.NonNumericValues++
				continue
			}
			fieldAggregate.PresentClusters++
			fieldAggregate.ObservedValue += value
		}

		fieldAggregate.LowerBound = fieldAggregate.ObservedValue
		// The denominator is the full explicit target set, including clusters that produced no
		// receipt (not ready, unreachable, stale generation, ...).
		fieldAggregate.MissingClusters = coverage.TargetClusters - fieldAggregate.PresentClusters
		if fieldAggregate.MissingClusters < 0 {
			fieldAggregate.MissingClusters = 0
		}

		if !coverage.Truncated && fieldAggregate.MissingClusters == 0 &&
			fieldAggregate.NonNumericValues == 0 && coverage.Complete {
			upperBound := fieldAggregate.ObservedValue
			fieldAggregate.UpperBound = &upperBound
			fieldAggregate.Complete = true
		}

		aggregated.Fields = append(aggregated.Fields, fieldAggregate)
	}

	return aggregated, nil
}

// FindField returns the aggregated field for the given path, or nil when the path was not collected.
func (a *AggregatedCoverage) FindField(path string) *AggregatedFieldCoverage {
	if a == nil {
		return nil
	}
	for i := range a.Fields {
		if a.Fields[i].Path == path {
			return &a.Fields[i]
		}
	}
	return nil
}

func findField(fields []fedcorev1a1.FieldCoverage, path string) *fedcorev1a1.FieldCoverage {
	for i := range fields {
		if fields[i].Path == path {
			return &fields[i]
		}
	}
	return nil
}

// integerValue extracts an integral JSON number. Real zeroes decode as 0 and are therefore
// distinguishable from a missing field, which never reaches this function.
func integerValue(raw *apiextensionsv1.JSON) (int64, bool, error) {
	var value any
	if err := json.Unmarshal(raw.Raw, &value); err != nil {
		return 0, false, err
	}
	number, ok := value.(float64)
	if !ok {
		return 0, false, nil
	}
	if number != float64(int64(number)) {
		return 0, false, fmt.Errorf("non-integral numeric value %v", number)
	}
	return int64(number), true, nil
}
