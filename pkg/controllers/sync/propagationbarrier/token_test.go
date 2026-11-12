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
	"errors"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"

	fedcorev1a1 "github.com/kubewharf/kubeadmiral/pkg/apis/core/v1alpha1"
)

func TestDigestStabilityAndEquivalence(t *testing.T) {
	template := newTemplate(1)

	digest1, err := DigestTemplate(template)
	if err != nil {
		t.Fatalf("digest template: %v", err)
	}
	digest2, err := DigestTemplate(newTemplate(1))
	if err != nil {
		t.Fatalf("digest template: %v", err)
	}
	if digest1 != digest2 {
		t.Fatalf("identical templates must produce identical digests: %q != %q", digest1, digest2)
	}
	if len(digest1) != 32 {
		t.Fatalf("md5 digest expected to be 32 hex chars, got %d", len(digest1))
	}

	changed := newTemplate(2)
	changedDigest, err := DigestTemplate(changed)
	if err != nil {
		t.Fatalf("digest changed template: %v", err)
	}
	if changedDigest == digest1 {
		t.Fatal("template change must change the digest")
	}

	overrideDigest1, err := DigestOverrides(nil)
	if err != nil {
		t.Fatalf("digest overrides: %v", err)
	}
	overrideDigest2, err := DigestOverrides(nil)
	if err != nil {
		t.Fatalf("digest overrides: %v", err)
	}
	if overrideDigest1 != overrideDigest2 {
		t.Fatal("identical override sets must produce identical digests")
	}
	// This mirrors the sync controller: null and [] serialize differently, so
	// their override versions differ as well.
	emptyOverrideDigest, err := DigestOverrides([]fedcorev1a1.OverrideWithController{})
	if err != nil {
		t.Fatalf("digest overrides: %v", err)
	}
	if emptyOverrideDigest == overrideDigest1 {
		t.Fatal("null and empty override sets must not share a digest")
	}

	overrides := []fedcorev1a1.OverrideWithController{{Controller: "test"}}
	changedOverrideDigest, err := DigestOverrides(overrides)
	if err != nil {
		t.Fatalf("digest overrides: %v", err)
	}
	if changedOverrideDigest == emptyOverrideDigest {
		t.Fatal("override change must change the digest")
	}
}

func TestDigestTemplateRejectsNil(t *testing.T) {
	if _, err := DigestTemplate(nil); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput, got %v", err)
	}
}

func validIssueInput() IssueTokenInput {
	return IssueTokenInput{
		ObjectRef:      defaultRef(),
		FederatedUID:   testFedUID,
		Generation:     3,
		TemplateDigest: "template-digest",
		OverrideDigest: "override-digest",
		Targets: []PropagationTarget{
			{ClusterName: testClusterA, ClusterUID: testUIDA},
			{ClusterName: testClusterB, ClusterUID: testUIDB},
		},
	}
}

func TestNewPropagationBarrierValidation(t *testing.T) {
	clock := newFakeClock()
	limits := testLimits(time.Second)

	t.Run("rejects missing identity", func(t *testing.T) {
		input := validIssueInput()
		input.FederatedUID = ""
		if _, err := newPropagationBarrier(input, clock.Now(), limits); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("expected ErrInvalidInput, got %v", err)
		}
	})

	t.Run("rejects empty targets", func(t *testing.T) {
		input := validIssueInput()
		input.Targets = nil
		if _, err := newPropagationBarrier(input, clock.Now(), limits); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("expected ErrInvalidInput, got %v", err)
		}
	})

	t.Run("rejects too many targets", func(t *testing.T) {
		input := validIssueInput()
		input.Targets = []PropagationTarget{
			{ClusterName: "c1", ClusterUID: "u1"},
			{ClusterName: "c2", ClusterUID: "u2"},
			{ClusterName: "c3", ClusterUID: "u3"},
		}
		smallLimits := limits
		smallLimits.MaxTargets = 2
		if _, err := newPropagationBarrier(input, clock.Now(), smallLimits); !errors.Is(err, ErrTooManyTargets) {
			t.Fatalf("expected ErrTooManyTargets, got %v", err)
		}
	})

	t.Run("rejects duplicate target UIDs", func(t *testing.T) {
		input := validIssueInput()
		input.Targets = []PropagationTarget{
			{ClusterName: "c1", ClusterUID: "same-uid"},
			{ClusterName: "c2", ClusterUID: "same-uid"},
		}
		if _, err := newPropagationBarrier(input, clock.Now(), limits); !errors.Is(err, ErrDuplicateTarget) {
			t.Fatalf("expected ErrDuplicateTarget, got %v", err)
		}
	})

	t.Run("rejects TTL beyond max", func(t *testing.T) {
		input := validIssueInput()
		input.TTL = limits.MaxTTL + time.Second
		if _, err := newPropagationBarrier(input, clock.Now(), limits); !errors.Is(err, ErrTTLTooLarge) {
			t.Fatalf("expected ErrTTLTooLarge, got %v", err)
		}
	})

	t.Run("applies default TTL and unique token IDs", func(t *testing.T) {
		barrier1, err := newPropagationBarrier(validIssueInput(), clock.Now(), limits)
		if err != nil {
			t.Fatalf("issue barrier: %v", err)
		}
		barrier2, err := newPropagationBarrier(validIssueInput(), clock.Now(), limits)
		if err != nil {
			t.Fatalf("issue barrier: %v", err)
		}
		wantExpiry := clock.Now().Add(limits.DefaultTTL)
		if !barrier1.ExpiresAt.Equal(wantExpiry) {
			t.Fatalf("expected expiry %v, got %v", wantExpiry, barrier1.ExpiresAt)
		}
		if barrier1.TokenID == barrier2.TokenID {
			t.Fatal("token IDs must be unique")
		}
	})
}

func TestTokenEncodeVerifyRoundtrip(t *testing.T) {
	clock := newFakeClock()
	barrier, err := newPropagationBarrier(validIssueInput(), clock.Now(), testLimits(time.Second))
	if err != nil {
		t.Fatalf("issue barrier: %v", err)
	}
	key := []byte(testSigningKey)
	opaque, err := encodeToken(barrier, key)
	if err != nil {
		t.Fatalf("encode token: %v", err)
	}
	if strings.Count(opaque, ".") != 1 {
		t.Fatalf("opaque token must have the form payload.signature, got %q", opaque)
	}

	decoded, err := parseAndVerifyToken(opaque, key)
	if err != nil {
		t.Fatalf("verify token: %v", err)
	}
	if decoded.TokenID != barrier.TokenID || decoded.FederatedUID != barrier.FederatedUID ||
		decoded.Generation != barrier.Generation || len(decoded.Targets) != 2 {
		t.Fatalf("decoded token does not match original: %+v vs %+v", decoded, barrier)
	}
}

func TestTokenSignatureForgery(t *testing.T) {
	clock := newFakeClock()
	barrier, err := newPropagationBarrier(validIssueInput(), clock.Now(), testLimits(time.Second))
	if err != nil {
		t.Fatalf("issue barrier: %v", err)
	}
	opaque, err := encodeToken(barrier, []byte(testSigningKey))
	if err != nil {
		t.Fatalf("encode token: %v", err)
	}

	t.Run("wrong signing key", func(t *testing.T) {
		if _, err := parseAndVerifyToken(opaque, []byte("another-key")); !errors.Is(err, ErrInvalidToken) {
			t.Fatalf("expected ErrInvalidToken, got %v", err)
		}
	})

	t.Run("tampered payload", func(t *testing.T) {
		parts := strings.Split(opaque, ".")
		tampered := parts[0] + "x." + parts[1]
		if _, err := parseAndVerifyToken(tampered, []byte(testSigningKey)); !errors.Is(err, ErrInvalidToken) {
			t.Fatalf("expected ErrInvalidToken, got %v", err)
		}
	})

	t.Run("tampered signature", func(t *testing.T) {
		parts := strings.Split(opaque, ".")
		tampered := parts[0] + "." + parts[1] + "x"
		if _, err := parseAndVerifyToken(tampered, []byte(testSigningKey)); !errors.Is(err, ErrInvalidToken) {
			t.Fatalf("expected ErrInvalidToken, got %v", err)
		}
	})

	t.Run("garbage", func(t *testing.T) {
		for _, garbage := range []string{"", "no-dot", ".", "a.b.c"} {
			if _, err := parseAndVerifyToken(garbage, []byte(testSigningKey)); !errors.Is(err, ErrInvalidToken) {
				t.Fatalf("expected ErrInvalidToken for %q, got %v", garbage, err)
			}
		}
	})
}

func TestBarrierExpiry(t *testing.T) {
	clock := newFakeClock()
	barrier, err := newPropagationBarrier(validIssueInput(), clock.Now(), testLimits(time.Second))
	if err != nil {
		t.Fatalf("issue barrier: %v", err)
	}
	if barrier.Expired(clock.Now()) {
		t.Fatal("freshly issued token must not be expired")
	}
	clock.Advance(testLimits(time.Second).DefaultTTL)
	if !barrier.Expired(clock.Now()) {
		t.Fatal("token must be expired at and after its expiry time")
	}
}

func TestIssueRejectsMalformedInput(t *testing.T) {
	reader := newFakeStateReader()
	observer := newFakeMemberObjectObserver()
	clock := newFakeClock()
	manager := newTestManager(reader, observer, clock, testLimits(50*time.Millisecond))

	input := validIssueInput()
	input.ObjectRef.Name = ""
	if _, _, err := manager.Issue(input); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput, got %v", err)
	}

	input = validIssueInput()
	input.Targets[0].ClusterUID = types.UID("")
	if _, _, err := manager.Issue(input); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput, got %v", err)
	}
}
