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
*/

package preferspot

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	testprovider "k8s.io/autoscaler/cluster-autoscaler/cloudprovider/test"
	"k8s.io/autoscaler/cluster-autoscaler/expander"
	. "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	schedulercache "k8s.io/kubernetes/pkg/scheduler/cache"
)

func TestPreferSpot(t *testing.T) {
	provider := testprovider.NewTestCloudProvider(nil, nil)

	groupOptions := make(map[string]expander.Option)
	nodeInfos := make(map[string]*schedulercache.NodeInfo)

	for ngId, spot := range map[string]bool{
		"ondemand1": false,
		"ondemand2": false,
		"spot1":     true,
		"spot2":     true,
	} {
		provider.AddNodeGroup(ngId, 1, 10, 1)
		node := BuildTestNode(ngId, 1000, 1000)
		node.Labels["aws.amazon.com/spot"] = strconv.FormatBool(spot)
		provider.AddNode(ngId, node)

		nodeGroup, _ := provider.NodeGroupForNode(node)
		nodeInfo := schedulercache.NewNodeInfo()
		_ = nodeInfo.SetNode(node)

		groupOptions[ngId] = expander.Option{
			NodeGroup: nodeGroup,
			NodeCount: 1,
			Debug:     ngId,
		}
		nodeInfos[ngId] = nodeInfo
	}

	onDemand1Group := groupOptions["ondemand1"]
	onDemand2Group := groupOptions["ondemand2"]
	spot1Group := groupOptions["spot1"]
	spot2Group := groupOptions["spot2"]

	e := NewStrategy()

	// Both Spot and On Demand available, select Spot
	ret := e.BestOption([]expander.Option{onDemand1Group, onDemand2Group, spot1Group, spot2Group}, nodeInfos)
	assert.True(t, assert.ObjectsAreEqual(*ret, spot1Group) || assert.ObjectsAreEqual(*ret, spot2Group))

	// Only On Demand available
	ret2 := e.BestOption([]expander.Option{onDemand1Group, onDemand2Group}, nodeInfos)
	assert.True(t, assert.ObjectsAreEqual(*ret2, onDemand1Group) || assert.ObjectsAreEqual(*ret2, onDemand2Group))
}
