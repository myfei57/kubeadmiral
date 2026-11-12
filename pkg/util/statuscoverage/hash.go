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
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"

	fedcorev1a1 "github.com/kubewharf/kubeadmiral/pkg/apis/core/v1alpha1"
)

// ConfigHash deterministically identifies the status collection configuration (coverage strategy
// plus configured field paths). Two builds with the same configuration produce the same hash, so a
// configuration change can be detected by comparing against a previously published envelope.
func ConfigHash(strategy fedcorev1a1.CoverageStrategy, fields []string) string {
	normalized := append([]string(nil), fields...)
	sort.Strings(normalized)
	return digest(strings.Join([]string{string(strategy), strings.Join(normalized, ",")}, "|"))
}

// PlacementHash deterministically identifies an explicit target cluster set. Adding or removing a
// cluster changes the hash and marks previously published envelopes stale.
func PlacementHash(targetClusters []string) string {
	normalized := append([]string(nil), targetClusters...)
	sort.Strings(normalized)
	return digest(strings.Join(normalized, ","))
}

func digest(canonical string) string {
	sum := sha256.Sum256([]byte(canonical))
	return hex.EncodeToString(sum[:])
}
