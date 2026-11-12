/*
Copyright 2026 The KubeAdmiral Authors.

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

package override

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/sets"

	fedcorev1a1 "github.com/kubewharf/kubeadmiral/pkg/apis/core/v1alpha1"
	ctrutil "github.com/kubewharf/kubeadmiral/pkg/controllers/util"
)

// pointerRelation describes how two JSON pointers relate to each other.
type pointerRelation int

const (
	relationDisjoint pointerRelation = iota
	relationEqual
	relationAncestor
	relationDescendant
)

// arrayMutation records an array add/remove that relocates subsequent numeric indexes.
type arrayMutation struct {
	parent     []string
	add        bool
	index      int
	patchIndex int
}

// patchPointerInfo holds the static, execution-order pointer interpretation of one patch.
type patchPointerInfo struct {
	pathTokens    []string
	pathValid     bool
	writeTokens   [][]string
	readTokens    [][]string
	fromUncertain bool
}

// analyzePatchConflicts interprets the ordered raw patches against a replay of the base document.
// Array insertions and removals are tracked so that later indexes are mapped through the real
// relocations, and parent/child overlaps plus test/move/copy style dependencies are explained in
// execution order. Relationships that cannot be decided precisely are reported as UnknownConflict.
func analyzePatchConflicts(
	baseTemplateRaw []byte,
	planned []PlannedPatch,
) (sets.Set[string], sets.Set[string], []PatchConflict, map[int]string) {
	readSet := sets.New[string]()
	writeSet := sets.New[string]()
	conflicts := make([]PatchConflict, 0)
	effectivePaths := make(map[int]string)
	if len(planned) == 0 {
		return readSet, writeSet, conflicts, effectivePaths
	}

	infos := make([]patchPointerInfo, len(planned))
	for i := range planned {
		infos[i] = buildPointerInfo(planned[i].Patch)
		planned[i].ReadPaths = pointerSetToSortedList(infos[i].readTokens, readSet)
		planned[i].WritePaths = pointerSetToSortedList(infos[i].writeTokens, writeSet)
	}

	virtualDoc := &unstructured.Unstructured{}
	if err := virtualDoc.UnmarshalJSON(baseTemplateRaw); err != nil {
		// The template was already decoded successfully while building the plan, so this is unreachable.
		return readSet, writeSet, conflicts, effectivePaths
	}

	mutations := make([]arrayMutation, 0)
	degraded := false

	for j := range planned {
		info := infos[j]
		for _, writePath := range info.writeTokens {
			writeSet.Insert(joinJSONPointer(writePath))
		}

		// Compare against every earlier patch by mapping the current pointers backward through the
		// array mutations that happened in between.
		for i := 0; i < j; i++ {
			conflicts = append(conflicts, comparePatches(planned, infos, mutations, degraded, i, j)...)
		}

		// Deferred finalizer patches never drive the real candidate object, so they must not drive the
		// virtual replay either; their pointer relationships are still analyzed statically above.
		if planned[j].Deferred {
			continue
		}
		if !degraded && info.pathValid {
			if mutation, ok := recordArrayMutation(virtualDoc, planned[j].Patch, info); ok {
				mutation.patchIndex = j
				mutations = append(mutations, mutation)
			}
		}

		// Replay the applicable patches on a separate document so later pointer resolution observes
		// the real structure. A failure freezes the virtual document; subsequent relationships that
		// would need the frozen state are reported as unknown.
		if err := ctrutil.ApplyJSONPatch(virtualDoc, fedcorev1a1.OverridePatches{planned[j].Patch}); err != nil {
			degraded = true
		}
	}

	// Compute the final-document coordinate of each declared pointer. A remove always deletes its own
	// target node; all other pointers are mapped through the later array mutations.
	for j := range planned {
		if !infos[j].pathValid {
			continue
		}
		index := planned[j].Index
		if normalizedOp(planned[j].Patch) == OperatorRemove {
			effectivePaths[index] = removedEffectivePathMarker
			continue
		}
		laterMutations := make([]arrayMutation, 0)
		for _, mutation := range mutations {
			if mutation.patchIndex > j {
				laterMutations = append(laterMutations, mutation)
			}
		}
		mapped, removed, uncertain := mapTokensForward(infos[j].pathTokens, laterMutations)
		switch {
		case removed:
			effectivePaths[index] = removedEffectivePathMarker
		case uncertain:
			effectivePaths[index] = unknownEffectivePathMarker
		default:
			effective := joinJSONPointer(mapped)
			if effective != planned[j].Patch.Path {
				effectivePaths[index] = effective
			}
		}
	}

	return readSet, writeSet, conflicts, effectivePaths
}

func normalizedOp(patch fedcorev1a1.OverridePatch) string {
	if patch.Op == "" {
		return OperatorReplace
	}
	return patch.Op
}

func pointerSetToSortedList(tokens [][]string, aggregate sets.Set[string]) []string {
	local := sets.New[string]()
	for _, pointerTokens := range tokens {
		pointer := joinJSONPointer(pointerTokens)
		local.Insert(pointer)
		aggregate.Insert(pointer)
	}
	return sortedSetList(local)
}

func sortedSetList(set sets.Set[string]) []string {
	list := set.UnsortedList()
	sort.Strings(list)
	return list
}

// backwardResult maps a pointer expressed at patch j time back to the coordinate space that existed
// just before the earlier patch i executed.
type backwardResult struct {
	// tokens are the coordinates before the tracked mutations.
	tokens []string
	// introducedBy records the patch index that inserted the denoted array slot, or -1.
	introducedBy int
	// crossedAdds records the indexes of array inserts that displaced the denoted element.
	crossedAdds sets.Set[int]
	// uncertain is true when the mapping cannot be decided precisely.
	uncertain bool
}

// mapTokensBackward undoes the given array mutations in reverse execution order. It distinguishes the
// slot introduced by an insert from a pre-existing element displaced by that insert, which is what
// allows write/write conflicts and mere relocation dependencies to be told apart.
func mapTokensBackward(tokens []string, mutations []arrayMutation) backwardResult {
	result := backwardResult{
		tokens:       append([]string(nil), tokens...),
		introducedBy: -1,
		crossedAdds:  sets.New[int](),
	}
	for k := len(mutations) - 1; k >= 0; k-- {
		mutation := mutations[k]
		if len(result.tokens) <= len(mutation.parent) {
			continue
		}
		if !tokenPrefixEqual(result.tokens[:len(mutation.parent)], mutation.parent) {
			continue
		}
		token := result.tokens[len(mutation.parent)]
		index, err := parseArrayIndex(token)
		if err != nil {
			result.uncertain = true
			continue
		}
		if mutation.add {
			switch {
			case index == mutation.index:
				// The slot was created by this insert; it has no coordinate before the mutation.
				result.introducedBy = mutation.patchIndex
				return result
			case index > mutation.index:
				result.tokens[len(mutation.parent)] = strconv.Itoa(index - 1)
				result.crossedAdds.Insert(mutation.patchIndex)
			}
		} else if index >= mutation.index {
			// Undoing a removal: the current element used to live one slot further right.
			result.tokens[len(mutation.parent)] = strconv.Itoa(index + 1)
		}
	}
	return result
}

// comparePatches evaluates the relationship between earlier patch i and current patch j. Current
// pointers are mapped backward through the intervening array mutations so that node identity, rather
// than raw index spelling, drives the verdict.
func comparePatches(
	planned []PlannedPatch,
	infos []patchPointerInfo,
	mutations []arrayMutation,
	degraded bool,
	i, j int,
) []PatchConflict {
	earlier, current := planned[i], planned[j]
	earlierInfo, currentInfo := infos[i], infos[j]
	conflicts := make([]PatchConflict, 0)

	if !earlierInfo.pathValid || !currentInfo.pathValid {
		return conflicts
	}

	mutationsInRange := make([]arrayMutation, 0)
	for _, mutation := range mutations {
		if mutation.patchIndex >= i && mutation.patchIndex < j {
			mutationsInRange = append(mutationsInRange, mutation)
		}
	}
	// An add using the "-" token writes at a concrete resolved index; normalize the raw token so that
	// later numeric pointers can be compared against the real inserted slot.
	earlierWrites := normalizeDashWrites(earlier.Index, earlierInfo.writeTokens, mutationsInRange)

	// move/copy carry a "from" pointer that the OverridePatch schema cannot represent. Any dependency
	// involving it is therefore undecidable.
	if earlierInfo.fromUncertain || currentInfo.fromUncertain {
		if pointersOverlap(earlierInfo.writeTokens, currentInfo.writeTokens) ||
			pointersOverlap(earlierInfo.readTokens, currentInfo.writeTokens) ||
			pointersOverlap(earlierInfo.writeTokens, currentInfo.readTokens) {
			conflicts = append(conflicts, PatchConflict{
				Kind:              ConflictUnknown,
				EarlierPatchIndex: earlier.Index,
				EarlierSource:     earlier.Source,
				Path:              current.Patch.Path,
				Reason: fmt.Sprintf(
					"patch %d uses move/copy whose from pointer is not retained by the OverridePatch schema; dependency with patch %d cannot be decided",
					j, i,
				),
			})
		}
		return conflicts
	}

	// Current writes, mapped back to the earlier patch's coordinate space.
	for _, currentWrite := range currentInfo.writeTokens {
		backward := mapTokensBackward(currentWrite, mutationsInRange)
		if backward.uncertain || degraded {
			conflicts = append(conflicts, unknownConflict(earlier, current, currentWrite,
				"array relocation leading to this write cannot be resolved precisely"))
			continue
		}
		for _, earlierWrite := range earlierWrites {
			if !overlaps(backward.tokens, earlierWrite) {
				continue
			}
			switch {
			case backward.introducedBy == earlier.Index:
				conflicts = append(conflicts, PatchConflict{
					Kind:              ConflictWriteWrite,
					EarlierPatchIndex: earlier.Index,
					EarlierSource:     earlier.Source,
					Path:              joinJSONPointer(currentWrite),
					Reason: fmt.Sprintf(
						"write %q targets the node inserted earlier by patch %d; later patch wins under last-writer semantics",
						joinJSONPointer(currentWrite), earlier.Index,
					),
				})
			case backward.crossedAdds.Has(earlier.Index):
				conflicts = append(conflicts, PatchConflict{
					Kind:              ConflictDependency,
					EarlierPatchIndex: earlier.Index,
					EarlierSource:     earlier.Source,
					Path:              joinJSONPointer(currentWrite),
					Reason: fmt.Sprintf(
						"write %q reaches %q only after the array insert of patch %d relocates the element; ordering dependency on a different node",
						current.Patch.Path, joinJSONPointer(backward.tokens), earlier.Index,
					),
				})
			default:
				conflicts = append(conflicts, PatchConflict{
					Kind:              ConflictWriteWrite,
					EarlierPatchIndex: earlier.Index,
					EarlierSource:     earlier.Source,
					Path:              joinJSONPointer(currentWrite),
					Reason: fmt.Sprintf(
						"write %q (pre-coordinate %q) overlaps earlier write %q; later patch wins under last-writer semantics",
						current.Patch.Path, joinJSONPointer(backward.tokens), earlier.Patch.Path,
					),
				})
			}
		}
		for _, earlierRead := range earlierInfo.readTokens {
			if overlaps(backward.tokens, earlierRead) {
				conflicts = append(conflicts, PatchConflict{
					Kind:              ConflictDependency,
					EarlierPatchIndex: earlier.Index,
					EarlierSource:     earlier.Source,
					Path:              joinJSONPointer(currentWrite),
					Reason: fmt.Sprintf(
						"write %q changes a location read earlier by patch %d (pre-coordinate %q)",
						current.Patch.Path, earlier.Index, joinJSONPointer(backward.tokens),
					),
				})
			}
		}
	}

	// Current reads, mapped back to the earlier patch's coordinate space.
	for _, currentRead := range currentInfo.readTokens {
		backward := mapTokensBackward(currentRead, mutationsInRange)
		if backward.uncertain || degraded {
			conflicts = append(conflicts, unknownConflict(earlier, current, currentRead,
				"array relocation leading to this read cannot be resolved precisely"))
			continue
		}
		for _, earlierWrite := range earlierInfo.writeTokens {
			if overlaps(backward.tokens, earlierWrite) {
				conflicts = append(conflicts, PatchConflict{
					Kind:              ConflictDependency,
					EarlierPatchIndex: earlier.Index,
					EarlierSource:     earlier.Source,
					Path:              joinJSONPointer(currentRead),
					Reason: fmt.Sprintf(
						"read %q depends on earlier write %q (pre-coordinate %q)",
						current.Patch.Path, earlier.Patch.Path, joinJSONPointer(backward.tokens),
					),
				})
			}
		}
	}

	return dedupConflicts(conflicts)
}

// normalizeDashWrites rewrites a trailing "-" token of an add patch to the numeric index resolved at
// execution time, which is recorded on its array mutation.
func normalizeDashWrites(patchIndex int, writes [][]string, mutations []arrayMutation) [][]string {
	result := make([][]string, 0, len(writes))
	for _, write := range writes {
		normalized := append([]string(nil), write...)
		if len(normalized) > 0 && normalized[len(normalized)-1] == "-" {
			parent := normalized[:len(normalized)-1]
			for _, mutation := range mutations {
				if mutation.patchIndex == patchIndex && tokenPrefixEqual(mutation.parent, parent) {
					normalized[len(normalized)-1] = strconv.Itoa(mutation.index)
					break
				}
			}
		}
		result = append(result, normalized)
	}
	return result
}

func pointersOverlap(left, right [][]string) bool {
	for _, leftTokens := range left {
		for _, rightTokens := range right {
			if overlaps(leftTokens, rightTokens) {
				return true
			}
		}
	}
	return false
}

func unknownConflict(earlier, current PlannedPatch, currentTokens []string, reason string) PatchConflict {
	return PatchConflict{
		Kind:              ConflictUnknown,
		EarlierPatchIndex: earlier.Index,
		EarlierSource:     earlier.Source,
		Path:              joinJSONPointer(currentTokens),
		Reason:            reason,
	}
}

func dedupConflicts(conflicts []PatchConflict) []PatchConflict {
	seen := sets.New[string]()
	result := make([]PatchConflict, 0, len(conflicts))
	for _, conflict := range conflicts {
		key := fmt.Sprintf("%d|%s|%s", conflict.EarlierPatchIndex, conflict.Kind, conflict.Path)
		if seen.Has(key) {
			continue
		}
		seen.Insert(key)
		result = append(result, conflict)
	}
	return result
}

// buildPointerInfo derives the read and write pointer sets of one patch according to RFC 6902 op
// semantics. An empty op defaults to replace, matching ApplyJSONPatch.
func buildPointerInfo(patch fedcorev1a1.OverridePatch) patchPointerInfo {
	info := patchPointerInfo{}
	tokens, err := parseJSONPointer(patch.Path)
	if err != nil {
		return info
	}
	info.pathTokens = tokens
	info.pathValid = true

	op := patch.Op
	if op == "" {
		op = OperatorReplace
	}

	switch op {
	case OperatorAdd:
		info.writeTokens = [][]string{tokens}
		info.readTokens = parentPointers(tokens)
	case OperatorRemove:
		info.writeTokens = [][]string{tokens}
		info.readTokens = append(parentPointers(tokens), tokens)
	case OperatorReplace:
		info.writeTokens = [][]string{tokens}
		info.readTokens = [][]string{tokens}
	case "test":
		info.readTokens = [][]string{tokens}
	case "move", "copy":
		// The schema only retains op/path/value; the mandatory "from" pointer is unavailable.
		info.writeTokens = [][]string{tokens}
		info.readTokens = parentPointers(tokens)
		info.fromUncertain = true
	default:
		// Unknown operators cannot be interpreted statically.
		info.writeTokens = [][]string{tokens}
		info.fromUncertain = true
	}
	return info
}

// parentPointers returns the container pointer of a pointer, or nil for a root-level pointer.
func parentPointers(tokens []string) [][]string {
	if len(tokens) <= 1 {
		return nil
	}
	parent := append([]string(nil), tokens[:len(tokens)-1]...)
	return [][]string{parent}
}

// recordArrayMutation detects whether the patch mutates an array index and resolves the concrete
// insertion/removal index, expanding "-" against the live virtual document.
func recordArrayMutation(
	doc *unstructured.Unstructured,
	patch fedcorev1a1.OverridePatch,
	info patchPointerInfo,
) (arrayMutation, bool) {
	op := normalizedOp(patch)
	// move/copy additionally relocate their "from" pointer, which is not retained by the schema, so
	// only plain add/remove can be tracked precisely here.
	if op != OperatorAdd && op != OperatorRemove {
		return arrayMutation{}, false
	}
	if len(info.pathTokens) == 0 {
		return arrayMutation{}, false
	}
	parentTokens := info.pathTokens[:len(info.pathTokens)-1]
	token := info.pathTokens[len(info.pathTokens)-1]
	parent, found := walkTokens(doc.Object, parentTokens)
	if !found {
		return arrayMutation{}, false
	}
	array, ok := parent.([]interface{})
	if !ok {
		return arrayMutation{}, false
	}

	if token == "-" {
		return arrayMutation{parent: append([]string(nil), parentTokens...), add: op == OperatorAdd, index: len(array)}, true
	}
	index, err := parseArrayIndex(token)
	if err != nil || index < 0 || index > len(array) {
		return arrayMutation{}, false
	}
	return arrayMutation{
		parent: append([]string(nil), parentTokens...),
		add:    op != OperatorRemove,
		index:  index,
	}, true
}

// mapTokensForward maps a pointer expressed before the given mutations to its coordinate after them.
// removed means the denoted element was deleted; uncertain means the mapping cannot be decided.
func mapTokensForward(tokens []string, mutations []arrayMutation) (mapped []string, removed bool, uncertain bool) {
	mapped = append([]string(nil), tokens...)
	for _, mutation := range mutations {
		if len(mapped) <= len(mutation.parent) {
			continue
		}
		if !tokenPrefixEqual(mapped[:len(mutation.parent)], mutation.parent) {
			continue
		}
		token := mapped[len(mutation.parent)]
		index, err := parseArrayIndex(token)
		if err != nil {
			uncertain = true
			continue
		}
		switch {
		case mutation.add:
			if index >= mutation.index {
				mapped[len(mutation.parent)] = strconv.Itoa(index + 1)
			}
		case index == mutation.index:
			removed = true
		case index > mutation.index:
			mapped[len(mutation.parent)] = strconv.Itoa(index - 1)
		}
	}
	return mapped, removed, uncertain
}

func overlaps(a, b []string) bool {
	switch compareTokens(a, b) {
	case relationEqual, relationAncestor, relationDescendant:
		return true
	default:
		return false
	}
}

func compareTokens(a, b []string) pointerRelation {
	common := len(a)
	if len(b) < common {
		common = len(b)
	}
	if !tokenPrefixEqual(a[:common], b[:common]) {
		return relationDisjoint
	}
	switch {
	case len(a) == len(b):
		return relationEqual
	case len(a) < len(b):
		return relationAncestor
	default:
		return relationDescendant
	}
}

func tokenPrefixEqual(prefix, tokens []string) bool {
	if len(prefix) > len(tokens) {
		return false
	}
	for i := range prefix {
		if prefix[i] != tokens[i] {
			return false
		}
	}
	return true
}

// walkTokens resolves a pointer inside the generic JSON representation of the virtual document.
func walkTokens(node interface{}, tokens []string) (interface{}, bool) {
	current := node
	for _, token := range tokens {
		switch typed := current.(type) {
		case map[string]interface{}:
			next, ok := typed[token]
			if !ok {
				return nil, false
			}
			current = next
		case []interface{}:
			index, err := parseArrayIndex(token)
			if err != nil || index < 0 || index >= len(typed) {
				return nil, false
			}
			current = typed[index]
		default:
			return nil, false
		}
	}
	return current, true
}

// parseJSONPointer decodes an RFC 6901 pointer into unescaped tokens.
func parseJSONPointer(path string) ([]string, error) {
	if path == "" {
		return nil, errors.New("empty JSON pointer")
	}
	if !strings.HasPrefix(path, "/") {
		return nil, fmt.Errorf("JSON pointer %q must start with /", path)
	}
	if path == "/" {
		return []string{""}, nil
	}
	rawTokens := strings.Split(path[1:], "/")
	tokens := make([]string, len(rawTokens))
	for i, raw := range rawTokens {
		tokens[i] = unescapePointerToken(raw)
	}
	return tokens, nil
}

func unescapePointerToken(token string) string {
	token = strings.ReplaceAll(token, "~1", "/")
	token = strings.ReplaceAll(token, "~0", "~")
	return token
}

func escapePointerToken(token string) string {
	token = strings.ReplaceAll(token, "~", "~0")
	token = strings.ReplaceAll(token, "/", "~1")
	return token
}

func joinJSONPointer(tokens []string) string {
	if len(tokens) == 0 {
		return ""
	}
	escaped := make([]string, len(tokens))
	for i, token := range tokens {
		escaped[i] = escapePointerToken(token)
	}
	return "/" + strings.Join(escaped, "/")
}

func parseArrayIndex(token string) (int, error) {
	// RFC 6901 allows leading zero only for the single token "0".
	if token == "-" {
		return 0, fmt.Errorf("append token %q is not a numeric index", token)
	}
	if len(token) > 1 && token[0] == '0' {
		return 0, fmt.Errorf("invalid array index %q", token)
	}
	index, err := strconv.Atoi(token)
	if err != nil || index < 0 {
		return 0, fmt.Errorf("invalid array index %q", token)
	}
	return index, nil
}
