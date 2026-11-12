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

package status

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/klog/v2"

	fedcorev1a1 "github.com/kubewharf/kubeadmiral/pkg/apis/core/v1alpha1"
	"github.com/kubewharf/kubeadmiral/pkg/controllers/common"
	clusterutil "github.com/kubewharf/kubeadmiral/pkg/util/cluster"
	"github.com/kubewharf/kubeadmiral/pkg/util/informermanager"
	"github.com/kubewharf/kubeadmiral/pkg/util/statuscoverage"
)

// buildCoverage constructs the optional CollectedStatusCoverage envelope for the explicit target
// cluster set of fedObject. Unlike the legacy collected fields, which only cover ready clusters,
// every target cluster is visited: not-ready, unreachable or not-yet-synced clusters are recorded
// with explicit Unknown/Error states instead of disappearing from the result.
func (s *StatusController) buildCoverage(
	ctx context.Context,
	fedObject fedcorev1a1.GenericFederatedObject,
	typeConfig *fedcorev1a1.FederatedTypeConfig,
	targetGVK schema.GroupVersionKind,
	targetQualifiedName common.QualifiedName,
	previous *fedcorev1a1.CollectedStatusCoverage,
) *fedcorev1a1.CollectedStatusCoverage {
	logger := klog.FromContext(ctx)

	targetClusters := statuscoverage.TargetClusterNames(fedObject)

	joinedClusters, err := s.fedInformerManager.GetJoinedClusters()
	if err != nil {
		logger.Error(err, "Failed to list joined clusters for coverage")
	}
	clusterInfo := make(map[string]*fedcorev1a1.FederatedCluster, len(joinedClusters))
	for _, cluster := range joinedClusters {
		clusterInfo[cluster.Name] = cluster
	}

	observations := make([]statuscoverage.MemberObservation, 0, len(targetClusters))
	for _, clusterName := range targetClusters {
		observation := statuscoverage.MemberObservation{ClusterName: clusterName}
		if cluster, ok := clusterInfo[clusterName]; ok {
			observation.ClusterUID = cluster.UID
			observation.Ready = clusterutil.IsClusterReady(&cluster.Status)
		}

		// Even when the cluster is not ready we attempt the lookup so that stale same-named objects
		// can still be identified; lookup failures are carried as explicit observations.
		clusterObj, exist, err := informermanager.GetClusterObject(
			ctx,
			s.ftcManager,
			s.fedInformerManager,
			clusterName,
			targetQualifiedName,
			targetGVK,
		)
		switch {
		case err != nil:
			observation.ObjectError = err
		case !exist:
			observation.ObjectAbsent = true
		default:
			observation.Object = clusterObj
		}

		observations = append(observations, observation)
	}

	return statuscoverage.Build(fedObject, observations, statuscoverage.BuildOptions{
		Strategy:   typeConfig.GetCoverageStrategy(),
		Fields:     typeConfig.Spec.StatusCollection.Fields,
		SourceKind: targetGVK.Kind,
		Previous:   previous,
	})
}

// coverageFromStatus returns the previously published envelope, if any.
func coverageFromStatus(
	existingStatus fedcorev1a1.GenericCollectedStatusObject,
) *fedcorev1a1.CollectedStatusCoverage {
	if existingStatus == nil {
		return nil
	}
	return existingStatus.GetGenericCollectedStatus().Coverage
}
