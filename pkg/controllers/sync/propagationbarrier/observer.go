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
	"context"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	fedcorev1a1 "github.com/kubewharf/kubeadmiral/pkg/apis/core/v1alpha1"
	"github.com/kubewharf/kubeadmiral/pkg/controllers/common"
)

// StateReader provides the persistent observations that barrier evaluation is
// based on. Implementations serve informer caches and direct API reads only;
// in-flight dispatch state and CollectedStatus are intentionally not exposed
// here because neither can prove propagation of a pinned input identity.
//
// All methods are strictly read-only: waiting never triggers a write.
// A missing object is reported as (nil, nil), mirroring Kubernetes semantics;
// transport and API errors are returned as errors instead.
type StateReader interface {
	// GetFederatedObject returns the current FederatedObject or
	// ClusterFederatedObject identified by ref. A nil object with a nil error
	// means the object no longer exists.
	GetFederatedObject(ctx context.Context, ref ObjectRef) (fedcorev1a1.GenericFederatedObject, error)
	// GetPropagatedVersion returns the persisted PropagatedVersion status of
	// the same-named version resource. A nil status with a nil error means no
	// version resource exists yet, i.e. there is no dispatch receipt at all.
	GetPropagatedVersion(ctx context.Context, ref ObjectRef) (*fedcorev1a1.PropagatedVersionStatus, error)
	// GetFederatedCluster returns the current FederatedCluster with the given
	// name. A nil cluster with a nil error means the cluster is gone.
	GetFederatedCluster(ctx context.Context, clusterName string) (*fedcorev1a1.FederatedCluster, error)
}

// MemberObjectObserver observes the actual object in a member cluster.
// Implementations prefer the informer cache and fall back to a direct GET, so
// a transient informer lag never masquerades as a missing member object.
//
// found=false with a nil error means the managed object is currently not
// observed; callers distinguish this from an explicit error.
type MemberObjectObserver interface {
	ObserveMemberObject(
		ctx context.Context,
		clusterName string,
		sourceGVK schema.GroupVersionKind,
		targetName common.QualifiedName,
	) (obj *unstructured.Unstructured, found bool, err error)
}
