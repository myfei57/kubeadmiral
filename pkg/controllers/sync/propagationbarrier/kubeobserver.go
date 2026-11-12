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
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	fedcorev1a1 "github.com/kubewharf/kubeadmiral/pkg/apis/core/v1alpha1"
	fedcorev1a1client "github.com/kubewharf/kubeadmiral/pkg/client/clientset/versioned/typed/core/v1alpha1"
	fedcorev1a1informers "github.com/kubewharf/kubeadmiral/pkg/client/informers/externalversions/core/v1alpha1"
	fedcorev1a1listers "github.com/kubewharf/kubeadmiral/pkg/client/listers/core/v1alpha1"
	"github.com/kubewharf/kubeadmiral/pkg/controllers/common"
	"github.com/kubewharf/kubeadmiral/pkg/util/informermanager"
	"github.com/kubewharf/kubeadmiral/pkg/util/managedlabel"
)

// KubeStateReader implements StateReader on top of the existing federated
// informer listers, with a quorum-avoiding direct GET fallback when the cache
// has not observed an object yet.
type KubeStateReader struct {
	fedClient fedcorev1a1client.CoreV1alpha1Interface

	fedObjectLister        fedcorev1a1listers.FederatedObjectLister
	clusterFedObjectLister fedcorev1a1listers.ClusterFederatedObjectLister
	pvLister               fedcorev1a1listers.PropagatedVersionLister
	clusterPVLister        fedcorev1a1listers.ClusterPropagatedVersionLister
	clusterLister          fedcorev1a1listers.FederatedClusterLister
}

var _ StateReader = &KubeStateReader{}

func (r *KubeStateReader) GetFederatedObject(
	ctx context.Context,
	ref ObjectRef,
) (fedcorev1a1.GenericFederatedObject, error) {
	if ref.Namespace == "" {
		obj, err := r.clusterFedObjectLister.Get(ref.Name)
		if apierrors.IsNotFound(err) {
			obj, err = r.fedClient.ClusterFederatedObjects().Get(ctx, ref.Name, metav1.GetOptions{ResourceVersion: "0"})
		}
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		return obj, nil
	}

	obj, err := r.fedObjectLister.FederatedObjects(ref.Namespace).Get(ref.Name)
	if apierrors.IsNotFound(err) {
		obj, err = r.fedClient.FederatedObjects(ref.Namespace).Get(
			ctx, ref.Name, metav1.GetOptions{ResourceVersion: "0"},
		)
	}
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return obj, nil
}

func (r *KubeStateReader) GetPropagatedVersion(
	ctx context.Context,
	ref ObjectRef,
) (*fedcorev1a1.PropagatedVersionStatus, error) {
	if ref.Namespace == "" {
		obj, err := r.clusterPVLister.Get(ref.Name)
		if apierrors.IsNotFound(err) {
			obj, err = r.fedClient.ClusterPropagatedVersions().Get(
				ctx, ref.Name, metav1.GetOptions{ResourceVersion: "0"},
			)
		}
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		status := obj.Status
		return &status, nil
	}

	obj, err := r.pvLister.PropagatedVersions(ref.Namespace).Get(ref.Name)
	if apierrors.IsNotFound(err) {
		obj, err = r.fedClient.PropagatedVersions(ref.Namespace).Get(
			ctx, ref.Name, metav1.GetOptions{ResourceVersion: "0"},
		)
	}
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	status := obj.Status
	return &status, nil
}

func (r *KubeStateReader) GetFederatedCluster(
	ctx context.Context,
	clusterName string,
) (*fedcorev1a1.FederatedCluster, error) {
	cluster, err := r.clusterLister.Get(clusterName)
	if apierrors.IsNotFound(err) {
		cluster, err = r.fedClient.FederatedClusters().Get(
			ctx, clusterName, metav1.GetOptions{ResourceVersion: "0"},
		)
	}
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return cluster, nil
}

// KubeMemberObjectObserver implements MemberObjectObserver using the existing
// FederatedInformerManager: the synced informer cache is preferred and a
// direct member-cluster GET covers cache lag. It mirrors the established
// informermanager.GetClusterObject behavior, including the managed-label check.
type KubeMemberObjectObserver struct {
	ftcManager         informermanager.FederatedTypeConfigManager
	fedInformerManager informermanager.FederatedInformerManager
}

var _ MemberObjectObserver = &KubeMemberObjectObserver{}

func (o *KubeMemberObjectObserver) ObserveMemberObject(
	ctx context.Context,
	clusterName string,
	sourceGVK schema.GroupVersionKind,
	targetName common.QualifiedName,
) (*unstructured.Unstructured, bool, error) {
	if lister, hasSynced, exists := o.fedInformerManager.GetResourceLister(sourceGVK, clusterName); exists && hasSynced() {
		clusterObj, err := lister.Get(targetName.String())
		if apierrors.IsNotFound(err) {
			return nil, false, nil
		}
		if err != nil {
			return nil, false, err
		}
		obj := clusterObj.(*unstructured.Unstructured)
		if !managedlabel.HasManagedLabel(obj) {
			return nil, false, nil
		}
		return obj, true, nil
	}

	dynamicClient, exists := o.fedInformerManager.GetClusterDynamicClient(clusterName)
	if !exists {
		return nil, false, fmt.Errorf("cluster client does not exist for cluster %q", clusterName)
	}
	typeConfig, exists := o.ftcManager.GetResourceFTC(sourceGVK)
	if !exists {
		return nil, false, fmt.Errorf("federated type config does not exist for GVK %q", sourceGVK)
	}

	clusterObj, err := dynamicClient.Resource(typeConfig.GetSourceTypeGVR()).
		Namespace(targetName.Namespace).
		Get(ctx, targetName.Name, metav1.GetOptions{ResourceVersion: "0"})
	if apierrors.IsNotFound(err) || meta.IsNoMatchError(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("failed to get member object %q in cluster %q: %w",
			targetName.String(), clusterName, err)
	}
	if !managedlabel.HasManagedLabel(clusterObj) {
		return nil, false, nil
	}
	return clusterObj, true, nil
}

// InformerWaker translates host-cluster informer events into best-effort
// barrier wake signals. Member-cluster object events are not required here:
// receipt (PropagatedVersion) updates already wake the loop after dispatch and
// the periodic re-query covers everything else, including lost events.
type InformerWaker struct {
	manager *Manager
	logger  klog.Logger
}

// objectWakeHandler returns an event handler that wakes tokens waiting on the
// federated object or version resource named by the event object.
func objectWakeHandler(manager *Manager, logger klog.Logger) cache.ResourceEventHandler {
	wakeObject := func(observed interface{}) {
		accessor, err := meta.Accessor(observed)
		if err != nil {
			logger.Error(err, "Failed to access object metadata in barrier wake handler")
			return
		}
		manager.Wake(ObjectRef{Namespace: accessor.GetNamespace(), Name: accessor.GetName()})
	}
	return cache.ResourceEventHandlerFuncs{
		AddFunc:    wakeObject,
		UpdateFunc: func(_, newObj interface{}) { wakeObject(newObj) },
		DeleteFunc: func(deleted interface{}) {
			if tombstone, ok := deleted.(cache.DeletedFinalStateUnknown); ok {
				deleted = tombstone.Obj
			}
			wakeObject(deleted)
		},
	}
}

// clusterWakeHandler returns an event handler that wakes every token because a
// cluster event may affect targets of arbitrary federated objects.
func clusterWakeHandler(manager *Manager) cache.ResourceEventHandler {
	return cache.ResourceEventHandlerFuncs{
		AddFunc:    func(interface{}) { manager.WakeAll() },
		UpdateFunc: func(_, _ interface{}) { manager.WakeAll() },
		DeleteFunc: func(interface{}) { manager.WakeAll() },
	}
}

// Register attaches wake handlers to the supplied host-cluster informers.
func (w *InformerWaker) Register(
	fedObjectInformer fedcorev1a1informers.FederatedObjectInformer,
	clusterFedObjectInformer fedcorev1a1informers.ClusterFederatedObjectInformer,
	pvInformer fedcorev1a1informers.PropagatedVersionInformer,
	clusterPVInformer fedcorev1a1informers.ClusterPropagatedVersionInformer,
	clusterInformer fedcorev1a1informers.FederatedClusterInformer,
) {
	objectHandler := objectWakeHandler(w.manager, w.logger)
	clusterHandler := clusterWakeHandler(w.manager)

	register := func(name string, informer cache.SharedIndexInformer, handler cache.ResourceEventHandler) {
		if _, err := informer.AddEventHandler(handler); err != nil {
			w.logger.Error(err, "Failed to register barrier wake handler", "informer", name)
		}
	}
	register("federatedobjects", fedObjectInformer.Informer(), objectHandler)
	register("clusterfederatedobjects", clusterFedObjectInformer.Informer(), objectHandler)
	register("propagatedversions", pvInformer.Informer(), objectHandler)
	register("clusterpropagatedversions", clusterPVInformer.Informer(), objectHandler)
	register("federatedclusters", clusterInformer.Informer(), clusterHandler)
}

// KubeBarrierConfig groups the existing controller-manager dependencies
// required to construct a production barrier manager.
type KubeBarrierConfig struct {
	Name                             string
	FedClient                        fedcorev1a1client.CoreV1alpha1Interface
	FederatedObjectInformer          fedcorev1a1informers.FederatedObjectInformer
	ClusterFederatedObjectInformer   fedcorev1a1informers.ClusterFederatedObjectInformer
	PropagatedVersionInformer        fedcorev1a1informers.PropagatedVersionInformer
	ClusterPropagatedVersionInformer fedcorev1a1informers.ClusterPropagatedVersionInformer
	FederatedClusterInformer         fedcorev1a1informers.FederatedClusterInformer
	FTCManager                       informermanager.FederatedTypeConfigManager
	FederatedInformerManager         informermanager.FederatedInformerManager
	SigningKey                       []byte
	Limits                           Limits
	Logger                           klog.Logger
}

// NewKubeBarrierManager wires a barrier manager to the existing federated
// clients, listers and informer managers. It creates no new goroutines; callers
// must run Manager.Run to start the TTL sweeper.
func NewKubeBarrierManager(config KubeBarrierConfig) (*Manager, error) {
	if config.FedClient == nil || config.FTCManager == nil || config.FederatedInformerManager == nil {
		return nil, fmt.Errorf("%w: fed client, FTC manager and federated informer manager are required", ErrInvalidInput)
	}
	reader := &KubeStateReader{
		fedClient:              config.FedClient,
		fedObjectLister:        config.FederatedObjectInformer.Lister(),
		clusterFedObjectLister: config.ClusterFederatedObjectInformer.Lister(),
		pvLister:               config.PropagatedVersionInformer.Lister(),
		clusterPVLister:        config.ClusterPropagatedVersionInformer.Lister(),
		clusterLister:          config.FederatedClusterInformer.Lister(),
	}
	observer := &KubeMemberObjectObserver{
		ftcManager:         config.FTCManager,
		fedInformerManager: config.FederatedInformerManager,
	}
	manager := NewManager(config.Name, config.SigningKey, reader, observer, config.Limits, config.Logger)
	waker := &InformerWaker{manager: manager, logger: manager.logger}
	waker.Register(
		config.FederatedObjectInformer,
		config.ClusterFederatedObjectInformer,
		config.PropagatedVersionInformer,
		config.ClusterPropagatedVersionInformer,
		config.FederatedClusterInformer,
	)
	return manager, nil
}
