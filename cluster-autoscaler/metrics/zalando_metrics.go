/*
Copyright 2016 The Kubernetes Authors.

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

package metrics

import (
	"sync"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/autoscaler/cluster-autoscaler/clusterstate/api"
	k8smetrics "k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

const (
	nodeGroupNameLabel = "node_group"
)

var (
	poolNameLock = sync.Mutex{}
	poolNames    = sets.NewString()

	nodeGroupInBackoff = k8smetrics.NewGaugeVec(
		&k8smetrics.GaugeOpts{
			Namespace: caNamespace,
			Name:      "node_group_in_backoff",
			Help:      "Whether or not a node group is in BackOff. 1 if it is, 0 otherwise.",
		}, []string{nodeGroupNameLabel})
)

func registerZalandoMetrics() {
	legacyregistry.MustRegister(nodeGroupInBackoff)
}

func isBackedOff(nodeGroupStatus api.NodeGroupStatus) bool {
	for _, condition := range nodeGroupStatus.Conditions {
		if condition.Type == api.ClusterAutoscalerScaleUp {
			return condition.Status == api.ClusterAutoscalerBackoff
		}
	}
	return false
}

// UpdateNodeGroupMetrics updates the per-node group metrics
func UpdateNodeGroupMetrics(status *api.ClusterAutoscalerStatus) {
	poolNameLock.Lock()
	defer poolNameLock.Unlock()

	// Update the metrics
	newNames := sets.NewString()
	for _, groupStatus := range status.NodeGroupStatuses {
		newNames.Insert(groupStatus.ProviderID)

		value := 0.0
		if isBackedOff(groupStatus) {
			value = 1
		}
		nodeGroupInBackoff.With(map[string]string{nodeGroupNameLabel: groupStatus.ProviderID}).Set(value)
	}

	// Cleanup metrics for removed node groups
	for oldName := range poolNames {
		if !newNames.Has(oldName) {
			nodeGroupInBackoff.Delete(map[string]string{nodeGroupNameLabel: oldName})
		}
	}
	poolNames = newNames
}
