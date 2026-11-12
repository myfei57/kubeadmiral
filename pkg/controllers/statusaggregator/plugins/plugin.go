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

package plugins

import (
	"context"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	fedcorev1a1 "github.com/kubewharf/kubeadmiral/pkg/apis/core/v1alpha1"
	"github.com/kubewharf/kubeadmiral/pkg/controllers/common"
)

type Plugin interface {
	// AggregateStatuses aggregates member cluster object statues to source object status and returns the latter
	AggregateStatuses(
		ctx context.Context,
		sourceObject *unstructured.Unstructured,
		fedObject fedcorev1a1.GenericFederatedObject,
		clusterObjs map[string]interface{},
		clusterObjsUpToDate bool,
	) (*unstructured.Unstructured, bool, error)
}

// CoverageAwarePlugin is implemented by aggregator plugins that declare support for the optional
// CollectedStatusCoverage envelope. When an envelope is available and the plugin declares support,
// the status aggregator hands the envelope to the plugin instead of raw member cluster objects.
// The IsResourcePropagated gate result is still passed through: coverage never replaces or bypasses
// that gate.
type CoverageAwarePlugin interface {
	Plugin
	// SupportsCoverage declares that the plugin understands the coverage envelope protocol.
	SupportsCoverage() bool
	// AggregateCoverage aggregates the source object status from an explicit coverage envelope.
	// resourcePropagated is the retained IsResourcePropagated gate result.
	AggregateCoverage(
		ctx context.Context,
		sourceObject *unstructured.Unstructured,
		fedObject fedcorev1a1.GenericFederatedObject,
		coverage *fedcorev1a1.CollectedStatusCoverage,
		resourcePropagated bool,
	) (*unstructured.Unstructured, bool, error)
}

var pluginsMap = map[schema.GroupVersionKind]Plugin{
	appsv1.SchemeGroupVersion.WithKind(common.DeploymentKind):  NewDeploymentPlugin(),
	appsv1.SchemeGroupVersion.WithKind(common.StatefulSetKind): NewSingleClusterPlugin(),
	batchv1.SchemeGroupVersion.WithKind(common.JobKind):        NewJobPlugin(),
	corev1.SchemeGroupVersion.WithKind(common.PodKind):         NewPodPlugin(),
}

func GetPlugin(gvk schema.GroupVersionKind) Plugin {
	return pluginsMap[gvk]
}

// GetCoveragePlugin returns the coverage-aware plugin for the given GVK, or nil when no plugin
// declares envelope support. The status aggregator must fall back to the legacy aggregation path
// in that case.
func GetCoveragePlugin(gvk schema.GroupVersionKind) CoverageAwarePlugin {
	plugin, ok := pluginsMap[gvk].(CoverageAwarePlugin)
	if !ok || !plugin.SupportsCoverage() {
		return nil
	}
	return plugin
}
