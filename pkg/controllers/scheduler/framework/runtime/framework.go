/*
Copyright 2019 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

This file may have been modified by The KubeAdmiral Authors
("KubeAdmiral Modifications"). All KubeAdmiral Modifications
are Copyright 2023 The KubeAdmiral Authors.
*/

package runtime

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"

	fedcore "github.com/kubewharf/kubeadmiral/pkg/apis/core"
	fedcorev1a1 "github.com/kubewharf/kubeadmiral/pkg/apis/core/v1alpha1"
	"github.com/kubewharf/kubeadmiral/pkg/controllers/scheduler/framework"
	"github.com/kubewharf/kubeadmiral/pkg/controllers/scheduler/framework/trace"
	"github.com/kubewharf/kubeadmiral/pkg/stats"
	"github.com/kubewharf/kubeadmiral/pkg/stats/metrics"
)

const (
	filter         = "Filter"
	score          = "Score"
	selectClusters = "SelectClusters"
	replicas       = "Replicas"
)

type frameworkImpl struct {
	filterPlugins   []framework.FilterPlugin
	scorePlugins    []framework.ScorePlugin
	selectPlugins   []framework.SelectPlugin
	replicasPlugins []framework.ReplicasPlugin

	profileName string
	metrics     stats.Metrics
}

var _ framework.Framework = &frameworkImpl{}

func NewFramework(registry Registry, handle framework.Handle, enabledPlugins *fedcore.EnabledPlugins,
	profileName string, metrics stats.Metrics,
) (framework.Framework, error) {
	fwk := &frameworkImpl{}

	pluginsMap := make(map[string]framework.Plugin)

	for name, factory := range registry {
		if !enabledPlugins.IsPluginEnabled(name) {
			continue
		}
		plugin, err := factory(handle)
		if err != nil {
			return nil, fmt.Errorf("error initializing plugin %q: %w", name, err)
		}
		pluginsMap[name] = plugin
	}

	for _, e := range fwk.getExtensionPoints(enabledPlugins) {
		if err := addPlugins(e.slicePtr, e.plugins, pluginsMap); err != nil {
			return nil, err
		}
	}

	fwk.profileName = profileName
	fwk.metrics = metrics
	return fwk, nil
}

func addPlugins(pluginList interface{}, enabledPluginNames []string, pluginsMap map[string]framework.Plugin) error {
	plugins := reflect.ValueOf(pluginList).Elem()
	pluginType := plugins.Type().Elem()

	registeredPlugins := sets.New[string]()
	for _, pluginName := range enabledPluginNames {
		pg, ok := pluginsMap[pluginName]
		if !ok {
			return fmt.Errorf("%s %s does not exist", pluginType.Name(), pluginName)
		}

		if !reflect.TypeOf(pg).Implements(pluginType) {
			return fmt.Errorf("plugin %s does not implement %s", pluginName, pluginType.Name())
		}

		if registeredPlugins.Has(pluginName) {
			return fmt.Errorf("plugin %s already registered as %s", pluginName, pluginType.Name())
		}

		newPlugins := reflect.Append(plugins, reflect.ValueOf(pg))
		plugins.Set(newPlugins)
		registeredPlugins.Insert(pluginName)
	}

	return nil
}

// extensionPoint encapsulates desired and applied set of plugins at a specific extension point.
type extensionPoint struct {
	// the set of plugins to be configured at this extension point.
	plugins []string
	// a pointer to the slice storing plugins implementations that will run at this extension point.
	slicePtr interface{}
}

func (f *frameworkImpl) getExtensionPoints(enabledPlugins *fedcore.EnabledPlugins) []extensionPoint {
	return []extensionPoint{
		{plugins: enabledPlugins.FilterPlugins, slicePtr: &f.filterPlugins},
		{plugins: enabledPlugins.ScorePlugins, slicePtr: &f.scorePlugins},
		{plugins: enabledPlugins.SelectPlugins, slicePtr: &f.selectPlugins},
		{plugins: enabledPlugins.ReplicasPlugins, slicePtr: &f.replicasPlugins},
	}
}

func (f *frameworkImpl) RunFilterPlugins(
	ctx context.Context,
	schedulingUnit *framework.SchedulingUnit,
	cluster *fedcorev1a1.FederatedCluster,
) (result *framework.Result) {
	startTime := time.Now()
	defer func() {
		f.metrics.Duration(metrics.SchedulerFrameworkExtensionPointDuration, startTime,
			stats.Tag{Name: "extension_point", Value: filter},
			stats.Tag{Name: "profile", Value: f.profileName},
			stats.Tag{Name: "status", Value: result.Code().String()})
	}()
	observer := trace.ObserverFromContext(ctx)
	if observer != nil {
		observer.RecordFilterPluginOrder(pluginNames(f.filterPlugins))
	}
	for idx, pl := range f.filterPlugins {
		pluginResult := f.runFilterPlugin(ctx, pl, schedulingUnit, cluster)
		if observer != nil {
			observer.RecordFilterEval(cluster.Name, trace.PluginEval{
				Plugin:  pl.Name(),
				State:   trace.Evaluated,
				Code:    trace.ResultCodeOf(pluginResult),
				Opaque:  isOpaqueResult(pl, pluginResult),
				Reasons: safeReasons(pluginResult),
			})
		}
		if !pluginResult.IsSuccess() {
			pluginResult.SetFailedPlugin(pl.Name())
			if observer != nil {
				// The filter chain short-circuits here: record the remaining
				// plugins as NotEvaluated instead of pretending they ran.
				for _, skipped := range f.filterPlugins[idx+1:] {
					observer.RecordFilterEval(cluster.Name, trace.PluginEval{
						Plugin: skipped.Name(),
						State:  trace.NotEvaluated,
					})
				}
			}
			return pluginResult
		}
	}
	return framework.NewResult(framework.Success)
}

func (f *frameworkImpl) runFilterPlugin(
	ctx context.Context,
	pl framework.FilterPlugin,
	schedulingUnit *framework.SchedulingUnit,
	cluster *fedcorev1a1.FederatedCluster,
) (result *framework.Result) {
	startTime := time.Now()
	defer func() {
		f.metrics.Duration(metrics.SchedulerPluginExecutionDuration, startTime,
			stats.Tag{Name: "extension_point", Value: filter},
			stats.Tag{Name: "plugin", Value: pl.Name()},
			stats.Tag{Name: "status", Value: result.Code().String()})
	}()
	result = pl.Filter(ctx, schedulingUnit, cluster)
	return result
}

func (f *frameworkImpl) RunScorePlugins(
	ctx context.Context,
	schedulingUnit *framework.SchedulingUnit,
	clusters []*fedcorev1a1.FederatedCluster,
) (pluginToClusterScore framework.PluginToClusterScore, result *framework.Result) {
	startTime := time.Now()
	defer func() {
		f.metrics.Duration(metrics.SchedulerFrameworkExtensionPointDuration, startTime,
			stats.Tag{Name: "extension_point", Value: score},
			stats.Tag{Name: "profile", Value: f.profileName},
			stats.Tag{Name: "status", Value: result.Code().String()})
	}()
	observer := trace.ObserverFromContext(ctx)
	pluginToClusterScore = make(framework.PluginToClusterScore)

	for pluginIdx, plugin := range f.scorePlugins {
		scoreList := make(framework.ClusterScoreList, len(clusters))
		// Raw scores are snapshot as value copies before NormalizeScore can
		// mutate the shared scoreList in place.
		evals := make([]trace.ClusterScoreEval, 0, len(clusters))
		for i, cluster := range clusters {
			score, res := plugin.Score(ctx, schedulingUnit, cluster)
			if observer != nil {
				evals = append(evals, trace.ClusterScoreEval{
					Cluster:  cluster.Name,
					State:    trace.Evaluated,
					Code:     trace.ResultCodeOf(res),
					RawScore: score,
					Opaque:   isOpaqueResult(plugin, res),
					Reasons:  safeReasons(res),
				})
			}
			if !res.IsSuccess() {
				msg := fmt.Sprintf(
					"plugin %q schedulingUnit %s failed with %s",
					plugin.Name(),
					schedulingUnit.Key(),
					res.AsError(),
				)
				klog.Error(msg)
				if observer != nil {
					observer.RecordScorePlugin(trace.PluginScoreTrace{
						Plugin:         plugin.Name(),
						Weight:         effectivePluginScoreWeight,
						NormalizeState: trace.NotEvaluated,
						Clusters:       evals,
					})
					f.recordSkippedScorePlugins(observer, pluginIdx+1, clusters)
				}
				return nil, framework.NewResult(framework.Error, msg)
			}
			scoreList[i] = framework.ClusterScore{Cluster: cluster, Score: score}
		}

		normalizeState := trace.NotEvaluated
		if plugin.ScoreExtensions() != nil {
			res := plugin.ScoreExtensions().NormalizeScore(ctx, scoreList)
			if !res.IsSuccess() {
				msg := fmt.Sprintf(
					"plugin %q NormalizeScore schedulingUnit %s failed with %s",
					plugin.Name(),
					schedulingUnit.Key(),
					res.AsError(),
				)
				klog.Error(msg)
				if observer != nil {
					observer.RecordScorePlugin(trace.PluginScoreTrace{
						Plugin:         plugin.Name(),
						Weight:         effectivePluginScoreWeight,
						NormalizeState: trace.Evaluated,
						Clusters:       evals,
					})
					f.recordSkippedScorePlugins(observer, pluginIdx+1, clusters)
				}
				return nil, framework.NewResult(framework.Error, msg)
			}
			normalizeState = trace.Evaluated
		}

		if observer != nil {
			if normalizeState == trace.Evaluated {
				for i := range evals {
					normalized := scoreList[i].Score
					evals[i].NormalizedScore = &normalized
				}
			}
			observer.RecordScorePlugin(trace.PluginScoreTrace{
				Plugin:         plugin.Name(),
				Weight:         effectivePluginScoreWeight,
				NormalizeState: normalizeState,
				Clusters:       evals,
			})
		}

		pluginToClusterScore[plugin.Name()] = scoreList
	}

	return pluginToClusterScore, nil
}

// effectivePluginScoreWeight is the multiplier the framework applies to a
// plugin's score when aggregating totals. Scores are summed directly, so the
// effective weight is 1.
const effectivePluginScoreWeight int64 = 1

func (f *frameworkImpl) recordSkippedScorePlugins(
	observer *trace.Observer,
	startIdx int,
	clusters []*fedcorev1a1.FederatedCluster,
) {
	for _, plugin := range f.scorePlugins[startIdx:] {
		evals := make([]trace.ClusterScoreEval, 0, len(clusters))
		for _, cluster := range clusters {
			evals = append(evals, trace.ClusterScoreEval{
				Cluster: cluster.Name,
				State:   trace.NotEvaluated,
			})
		}
		observer.RecordScorePlugin(trace.PluginScoreTrace{
			Plugin:         plugin.Name(),
			Weight:         effectivePluginScoreWeight,
			NormalizeState: trace.NotEvaluated,
			Clusters:       evals,
		})
	}
}

func (f *frameworkImpl) RunSelectClustersPlugin(
	ctx context.Context,
	schedulingUnit *framework.SchedulingUnit,
	clusterScores framework.ClusterScoreList,
) (clusters []*fedcorev1a1.FederatedCluster, result *framework.Result) {
	startTime := time.Now()
	defer func() {
		f.metrics.Duration(metrics.SchedulerFrameworkExtensionPointDuration, startTime,
			stats.Tag{Name: "extension_point", Value: selectClusters},
			stats.Tag{Name: "profile", Value: f.profileName},
			stats.Tag{Name: "status", Value: result.Code().String()})
	}()
	observer := trace.ObserverFromContext(ctx)
	if observer != nil {
		observer.RecordSelectTieGroups(equalScoreGroups(clusterScores))
	}
	if len(f.selectPlugins) == 0 {
		for _, clusterScore := range clusterScores {
			clusters = append(clusters, clusterScore.Cluster)
		}
		result = framework.NewResult(framework.Success)
		if observer != nil {
			order := clusterScoreOrder(clusterScores)
			observer.RecordSelectPlugin(trace.SelectPluginTrace{
				Plugin:      "",
				State:       trace.NotEvaluated,
				InputOrder:  order,
				OutputOrder: order,
			})
		}
		return clusters, result
	}
	for idx, plugin := range f.selectPlugins {
		inputOrder := clusterScoreOrder(clusterScores)
		newClusterScores, result := plugin.SelectClusters(ctx, schedulingUnit, clusterScores)
		if !result.IsSuccess() {
			msg := fmt.Sprintf(
				"plugin %q failed to select clusters for schedulingUnit %s: %v",
				plugin.Name(),
				schedulingUnit.Key(),
				result.Message(),
			)
			klog.Error(msg)
			if observer != nil {
				observer.RecordSelectPlugin(trace.SelectPluginTrace{
					Plugin:      plugin.Name(),
					State:       trace.Evaluated,
					Code:        trace.CodeError,
					Opaque:      trace.IsOpaque(plugin),
					Reasons:     safeReasons(result),
					InputOrder:  inputOrder,
					OutputOrder: []string{},
					Eliminated:  inputOrder,
				})
				for _, skipped := range f.selectPlugins[idx+1:] {
					observer.RecordSelectPlugin(trace.SelectPluginTrace{
						Plugin:     skipped.Name(),
						State:      trace.NotEvaluated,
						InputOrder: inputOrder,
					})
				}
			}
			return nil, framework.NewResult(framework.Error, msg)
		}
		clusterScores = newClusterScores
		if observer != nil {
			outputOrder := clusterScoreOrder(newClusterScores)
			observer.RecordSelectPlugin(trace.SelectPluginTrace{
				Plugin:      plugin.Name(),
				State:       trace.Evaluated,
				Code:        trace.CodeSuccess,
				Opaque:      trace.IsOpaque(plugin),
				InputOrder:  inputOrder,
				OutputOrder: outputOrder,
				Eliminated:  eliminatedClusters(inputOrder, outputOrder),
			})
		}
	}

	for _, clusterScore := range clusterScores {
		clusters = append(clusters, clusterScore.Cluster)
	}
	return clusters, result
}

func (f *frameworkImpl) RunReplicasPlugin(
	ctx context.Context,
	schedulingUnit *framework.SchedulingUnit,
	clusters []*fedcorev1a1.FederatedCluster,
) (clusterReplicasList framework.ClusterReplicasList, result *framework.Result) {
	startTime := time.Now()
	defer func() {
		f.metrics.Duration(metrics.SchedulerFrameworkExtensionPointDuration, startTime,
			stats.Tag{Name: "extension_point", Value: replicas},
			stats.Tag{Name: "profile", Value: f.profileName},
			stats.Tag{Name: "status", Value: result.Code().String()})
	}()
	observer := trace.ObserverFromContext(ctx)
	desiredReplicas := int64(0)
	if schedulingUnit.DesiredReplicas != nil {
		desiredReplicas = *schedulingUnit.DesiredReplicas
	}
	if len(clusters) == 0 {
		result = framework.NewResult(
			framework.Success,
			"unnecessary to schedule due to no clusters",
		)
		if observer != nil {
			observer.RecordReplicasSkipped(result.Message())
		}
		return clusterReplicasList, result
	}
	if schedulingUnit.DesiredReplicas == nil || *schedulingUnit.DesiredReplicas <= 0 {
		result = framework.NewResult(
			framework.Success,
			"unnecessary to schedule due to replicas less or equal zero",
		)
		if observer != nil {
			observer.RecordReplicasSkipped(result.Message())
		}
		return clusterReplicasList, result
	}
	if len(f.replicasPlugins) == 0 {
		result = framework.NewResult(
			framework.Success,
			"no replicas plugin registered in the framework",
		)
		if observer != nil {
			observer.RecordReplicasSkipped(result.Message())
		}
		return clusterReplicasList, result
	}
	for _, plugin := range f.replicasPlugins {
		clusterReplicasList, result = plugin.ReplicaScheduling(ctx, schedulingUnit, clusters)
		if !result.IsSuccess() {
			msg := fmt.Sprintf(
				"plugin %q failed to replica scheduling for schedulingUnit %s: %v",
				plugin.Name(),
				schedulingUnit.Key(),
				result.Message(),
			)
			klog.Error(msg)
			if observer != nil {
				observer.RecordReplicasPlugin(plugin.Name(), trace.CodeError, desiredReplicas, nil)
			}
			return clusterReplicasList, framework.NewResult(framework.Error, msg)
		}
		if observer != nil {
			observer.RecordReplicasPlugin(
				plugin.Name(),
				trace.CodeSuccess,
				desiredReplicas,
				replicaAllocationMap(clusterReplicasList),
			)
		}
		return clusterReplicasList, result
	}
	return clusterReplicasList, result
}

func pluginNames[T framework.Plugin](plugins []T) []string {
	names := make([]string, 0, len(plugins))
	for _, plugin := range plugins {
		names = append(names, plugin.Name())
	}
	return names
}

func safeReasons(result *framework.Result) []string {
	if result == nil {
		return nil
	}
	return result.Reasons()
}

// isOpaqueResult marks observations of external plugins (e.g. scheduling
// webhooks) as opaque, as well as failures that carry no reason at all. The
// trace never synthesizes a cause in these cases.
func isOpaqueResult(plugin framework.Plugin, result *framework.Result) bool {
	if trace.IsOpaque(plugin) {
		return true
	}
	return result != nil && !result.IsSuccess() && len(result.Reasons()) == 0
}

func clusterScoreOrder(clusterScores framework.ClusterScoreList) []string {
	order := make([]string, 0, len(clusterScores))
	for _, clusterScore := range clusterScores {
		order = append(order, clusterScore.Cluster.Name)
	}
	return order
}

func eliminatedClusters(inputOrder, outputOrder []string) []string {
	if len(outputOrder) == len(inputOrder) {
		return nil
	}
	selected := sets.New(outputOrder...)
	eliminated := make([]string, 0)
	for _, cluster := range inputOrder {
		if !selected.Has(cluster) {
			eliminated = append(eliminated, cluster)
		}
	}
	return eliminated
}

// equalScoreGroups returns the groups of clusters that share an identical
// total score, in first-appearance order. Groups of one are omitted. The
// observed ordering inside each group is the only honest record of how ties
// were broken.
func equalScoreGroups(clusterScores framework.ClusterScoreList) [][]string {
	scoreToGroup := make(map[int64][]string)
	order := make([]int64, 0)
	for _, clusterScore := range clusterScores {
		if _, exists := scoreToGroup[clusterScore.Score]; !exists {
			order = append(order, clusterScore.Score)
		}
		scoreToGroup[clusterScore.Score] = append(scoreToGroup[clusterScore.Score], clusterScore.Cluster.Name)
	}
	groups := make([][]string, 0)
	for _, score := range order {
		group := scoreToGroup[score]
		if len(group) > 1 {
			groups = append(groups, group)
		}
	}
	return groups
}

func replicaAllocationMap(clusterReplicasList framework.ClusterReplicasList) map[string]int64 {
	allocation := make(map[string]int64, len(clusterReplicasList))
	for _, clusterReplicas := range clusterReplicasList {
		allocation[clusterReplicas.Cluster.Name] = clusterReplicas.Replicas
	}
	return allocation
}
