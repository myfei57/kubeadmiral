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
	"crypto/hmac"
	"crypto/md5" //nolint:gosec // md5 matches the sync controller version hashing
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	fedcorev1a1 "github.com/kubewharf/kubeadmiral/pkg/apis/core/v1alpha1"
)

// tokenFormatVersion is the current PropagationBarrier encoding version.
const tokenFormatVersion = 1

// Sentinel errors returned by token and manager operations. Callers can use
// errors.Is to distinguish rejection reasons from ordinary wait outcomes.
var (
	// ErrInvalidToken is returned when an opaque token cannot be parsed or its
	// signature does not verify.
	ErrInvalidToken = errors.New("invalid propagation barrier token")
	// ErrUnknownToken is returned when the manager holds no server-side state
	// for the token, e.g. it was never issued by this manager or already expired.
	ErrUnknownToken = errors.New("unknown propagation barrier token")
	// ErrTokenExpired is returned when the token TTL has elapsed.
	ErrTokenExpired = errors.New("propagation barrier token expired")
	// ErrTooManyTargets is returned when a token requests more than MaxTargets.
	ErrTooManyTargets = errors.New("propagation barrier target limit exceeded")
	// ErrDuplicateTarget is returned when two targets carry the same cluster UID.
	ErrDuplicateTarget = errors.New("duplicate propagation barrier target UID")
	// ErrTTLTooLarge is returned when the requested TTL exceeds MaxTTL.
	ErrTTLTooLarge = errors.New("propagation barrier TTL limit exceeded")
	// ErrTooManyWaiters is returned when a token already has MaxWaitersPerToken.
	ErrTooManyWaiters = errors.New("propagation barrier waiter limit exceeded")
	// ErrTooManyTokens is returned when the manager already holds MaxTokens.
	ErrTooManyTokens = errors.New("propagation barrier token limit exceeded")
	// ErrQueueLimitExceeded is returned when the estimated queue byte budget is exhausted.
	ErrQueueLimitExceeded = errors.New("propagation barrier queue byte limit exceeded")
	// ErrManagerClosed is returned when the manager has been shut down.
	ErrManagerClosed = errors.New("propagation barrier manager closed")
	// ErrInvalidInput is returned for malformed issue requests.
	ErrInvalidInput = errors.New("invalid propagation barrier input")
)

// IssueTokenInput contains the immutable input identity a token is issued for.
type IssueTokenInput struct {
	// ObjectRef is the qualified name of the federated object.
	ObjectRef ObjectRef
	// FederatedGVK is the GVK of the federated object (FO or CFO).
	FederatedGVK schema.GroupVersionKind
	// SourceGVK is the GVK of the propagated workload.
	SourceGVK schema.GroupVersionKind
	// FederatedUID is the UID of the federated object instance.
	FederatedUID types.UID
	// Generation is the federated object generation to pin.
	Generation int64
	// TemplateDigest and OverrideDigest are obtained from DigestTemplate and
	// DigestOverrides for the federated object's current spec.
	TemplateDigest string
	OverrideDigest string
	// Targets is the explicit UID-pinned cluster set.
	Targets []PropagationTarget
	// TTL optionally overrides the manager default TTL and is capped by MaxTTL.
	TTL time.Duration
}

// DigestTemplate computes the template digest. The algorithm is identical to
// the template version hashing performed by the sync controller so that token
// digests can be compared directly with PropagatedVersion template versions.
func DigestTemplate(template *unstructured.Unstructured) (string, error) {
	if template == nil {
		return "", fmt.Errorf("%w: template is required", ErrInvalidInput)
	}
	jsonBytes, err := template.MarshalJSON()
	if err != nil {
		return "", fmt.Errorf("failed to marshal template: %w", err)
	}
	return md5Hex(jsonBytes), nil
}

// DigestOverrides computes the digest of the full federated override set. The
// wrapping structure matches the sync controller's override version hashing so
// the digest can be compared directly with PropagatedVersion override versions.
func DigestOverrides(overrides []fedcorev1a1.OverrideWithController) (string, error) {
	obj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"overrides": overrides,
		},
	}
	jsonBytes, err := obj.MarshalJSON()
	if err != nil {
		return "", fmt.Errorf("failed to marshal overrides: %w", err)
	}
	return md5Hex(jsonBytes), nil
}

//nolint:gosec
func md5Hex(payload []byte) string {
	hash := md5.New()
	_, _ = hash.Write(payload)
	return hex.EncodeToString(hash.Sum(nil))
}

// newPropagationBarrier validates the input and builds an unsigned barrier.
func newPropagationBarrier(input IssueTokenInput, now time.Time, limits Limits) (*PropagationBarrier, error) {
	if input.ObjectRef.Name == "" || input.FederatedUID == "" {
		return nil, fmt.Errorf("%w: object reference and federated UID are required", ErrInvalidInput)
	}
	if input.TemplateDigest == "" || input.OverrideDigest == "" {
		return nil, fmt.Errorf("%w: template and override digests are required", ErrInvalidInput)
	}
	if len(input.Targets) == 0 {
		return nil, fmt.Errorf("%w: at least one target is required", ErrInvalidInput)
	}
	if len(input.Targets) > limits.MaxTargets {
		return nil, fmt.Errorf("%w: %d > %d", ErrTooManyTargets, len(input.Targets), limits.MaxTargets)
	}

	seenUIDs := make(map[types.UID]struct{}, len(input.Targets))
	seenNames := make(map[string]struct{}, len(input.Targets))
	targets := make([]PropagationTarget, 0, len(input.Targets))
	for _, target := range input.Targets {
		if target.ClusterName == "" || target.ClusterUID == "" {
			return nil, fmt.Errorf("%w: cluster name and UID are required for every target", ErrInvalidInput)
		}
		if _, exists := seenUIDs[target.ClusterUID]; exists {
			return nil, fmt.Errorf("%w: %s", ErrDuplicateTarget, target.ClusterUID)
		}
		if _, exists := seenNames[target.ClusterName]; exists {
			return nil, fmt.Errorf("%w: cluster name %s appears more than once", ErrInvalidInput, target.ClusterName)
		}
		seenUIDs[target.ClusterUID] = struct{}{}
		seenNames[target.ClusterName] = struct{}{}
		targets = append(targets, target)
	}

	ttl := input.TTL
	if ttl <= 0 {
		ttl = limits.DefaultTTL
	}
	if ttl > limits.MaxTTL {
		return nil, fmt.Errorf("%w: %s > %s", ErrTTLTooLarge, ttl, limits.MaxTTL)
	}

	tokenID, err := randomTokenID()
	if err != nil {
		return nil, fmt.Errorf("failed to generate token id: %w", err)
	}

	return &PropagationBarrier{
		Version:        tokenFormatVersion,
		TokenID:        tokenID,
		ObjectRef:      input.ObjectRef,
		FederatedGVK:   input.FederatedGVK,
		SourceGVK:      input.SourceGVK,
		FederatedUID:   input.FederatedUID,
		Generation:     input.Generation,
		TemplateDigest: input.TemplateDigest,
		OverrideDigest: input.OverrideDigest,
		Targets:        targets,
		IssuedAt:       now.UTC(),
		ExpiresAt:      now.Add(ttl).UTC(),
	}, nil
}

func randomTokenID() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

// randomSigningKey generates a per-process HMAC key. Tokens and waiter state
// are in-memory and therefore do not survive a manager restart, so a random
// key is sufficient and avoids persisting another secret.
func randomSigningKey() []byte {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		// rand.Read failing is not recoverable in any meaningful way here.
		panic(fmt.Errorf("failed to generate propagation barrier signing key: %w", err))
	}
	return buf
}

// encodeToken serializes the barrier and signs the payload with HMAC-SHA256,
// producing an opaque "payload.signature" string. The signing key never leaves
// the manager, so the token carries no authority beyond what the issuing
// manager grants it (opaque server-side state with the existing RBAC boundary
// of whoever can call the issuing API).
func encodeToken(barrier *PropagationBarrier, signingKey []byte) (string, error) {
	payload, err := json.Marshal(barrier)
	if err != nil {
		return "", fmt.Errorf("failed to marshal barrier token: %w", err)
	}
	encodedPayload := base64.RawURLEncoding.EncodeToString(payload)
	mac := hmac.New(sha256.New, signingKey)
	_, _ = mac.Write([]byte(encodedPayload))
	signature := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return encodedPayload + "." + signature, nil
}

// parseAndVerifyToken decodes an opaque token and verifies its signature in
// constant time. A malformed or forged token yields ErrInvalidToken without
// leaking which check failed.
func parseAndVerifyToken(opaque string, signingKey []byte) (*PropagationBarrier, error) {
	parts := strings.Split(opaque, ".")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return nil, ErrInvalidToken
	}
	mac := hmac.New(sha256.New, signingKey)
	_, _ = mac.Write([]byte(parts[0]))
	expectedSignature := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(expectedSignature), []byte(parts[1])) {
		return nil, ErrInvalidToken
	}

	payload, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidToken, err)
	}
	barrier := &PropagationBarrier{}
	if err := json.Unmarshal(payload, barrier); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidToken, err)
	}
	if barrier.Version != tokenFormatVersion || barrier.TokenID == "" || len(barrier.Targets) == 0 {
		return nil, ErrInvalidToken
	}
	return barrier, nil
}
