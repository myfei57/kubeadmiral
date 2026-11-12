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
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

const (
	secretKind       = "Secret"
	secretDataField  = "data"
	secretStringData = "stringData"
	envNameField     = "name"
	envValueField    = "value"
)

// redactedCandidate returns a deep copy of the candidate with secret-like scalar values replaced by a
// fixed marker, and reports whether any redaction happened. The original object is never modified.
func redactedCandidate(obj *unstructured.Unstructured) (*unstructured.Unstructured, bool) {
	redacted := obj.DeepCopy()
	didRedact := redactNode(redacted.Object, false)
	if obj.GetKind() == secretKind {
		didRedact = redactSecretFields(redacted.Object) || didRedact
	}
	return redacted, didRedact
}

// redactSecretFields redacts every value under the conventional Secret data and stringData maps.
func redactSecretFields(object map[string]interface{}) bool {
	didRedact := false
	for _, field := range []string{secretDataField, secretStringData} {
		values, ok := object[field].(map[string]interface{})
		if !ok {
			continue
		}
		for key := range values {
			values[key] = redactionMarker
			didRedact = true
		}
	}
	return didRedact
}

// redactNode walks the generic JSON representation. A map key containing "secret" forces redaction of
// all scalar leaves beneath it, and container env entries whose name looks sensitive get their value
// redacted. forceRedact is inherited from a secret-like ancestor key.
func redactNode(node interface{}, forceRedact bool) bool {
	didRedact := false
	switch typed := node.(type) {
	case map[string]interface{}:
		if isEnvEntry(typed) {
			if forceRedact || isSensitiveEnvName(stringValue(typed[envNameField])) {
				if _, ok := typed[envValueField]; ok {
					typed[envValueField] = redactionMarker
					didRedact = true
				}
			}
		}
		for key, child := range typed {
			childForce := forceRedact || isSecretLikeKey(key)
			if childForce && isScalar(child) {
				typed[key] = redactionMarker
				didRedact = true
				continue
			}
			if redactNode(child, childForce) {
				didRedact = true
			}
		}
	case []interface{}:
		for _, child := range typed {
			if redactNode(child, forceRedact) {
				didRedact = true
			}
		}
	}
	return didRedact
}

func isEnvEntry(object map[string]interface{}) bool {
	_, hasName := object[envNameField]
	_, hasValue := object[envValueField]
	return hasName && hasValue
}

func isSensitiveEnvName(name string) bool {
	lower := strings.ToLower(name)
	for _, keyword := range []string{"secret", "password", "passwd", "token", "credential"} {
		if strings.Contains(lower, keyword) {
			return true
		}
	}
	return false
}

func isSecretLikeKey(key string) bool {
	return strings.Contains(strings.ToLower(key), "secret")
}

func isScalar(value interface{}) bool {
	switch value.(type) {
	case map[string]interface{}, []interface{}:
		return false
	default:
		return value != nil
	}
}

func stringValue(value interface{}) string {
	text, _ := value.(string)
	return text
}
