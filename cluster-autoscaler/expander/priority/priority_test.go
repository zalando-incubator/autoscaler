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

package priority

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	testprovider "k8s.io/autoscaler/cluster-autoscaler/cloudprovider/test"
	"k8s.io/autoscaler/cluster-autoscaler/expander"
	. "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	schedulernodeinfo "k8s.io/kubernetes/pkg/scheduler/nodeinfo"
)

func TestPriorityBased(t *testing.T) {
	provider := testprovider.NewTestCloudProvider(nil, nil)

	groupOptions := make(map[string]expander.Option)
	nodeInfos := make(map[string]*schedulernodeinfo.NodeInfo)

	for ngId, priority := range map[string]*int64{
		"highPriority":      intPtr(200),
		"veryHighPriority":  intPtr(300),
		"veryHighPriority2": intPtr(300),
		"noPriority":        nil,
		"negativePriority":  intPtr(-100),
		"lowPriority":       intPtr(100),
	} {
		provider.AddNodeGroup(ngId, 1, 10, 1)
		node := BuildTestNode(ngId, 1000, 1000)
		if priority != nil {
			node.Labels["zalando.org/scaling-priority"] = strconv.FormatInt(*priority, 10)
		}
		provider.AddNode(ngId, node)

		nodeGroup, _ := provider.NodeGroupForNode(node)
		nodeInfo := schedulernodeinfo.NewNodeInfo()
		_ = nodeInfo.SetNode(node)

		groupOptions[ngId] = expander.Option{
			NodeGroup: nodeGroup,
			NodeCount: 1,
			Debug:     ngId,
		}
		nodeInfos[ngId] = nodeInfo
	}

	var (
		negativePriorityGroup  = groupOptions["negativePriority"]
		noPriorityGroup        = groupOptions["noPriority"]
		lowPriorityGroup       = groupOptions["lowPriority"]
		highPriorityGroup      = groupOptions["highPriority"]
		veryHighPriorityGroup  = groupOptions["veryHighPriority"]
		veryHighPriorityGroup2 = groupOptions["veryHighPriority2"]
	)

	e := NewStrategy()

	// if there's no available options we return nil
	ret := e.BestOption([]expander.Option{}, nodeInfos)
	assert.Nil(t, ret)

	// if there's only one group we return that group
	ret = e.BestOption([]expander.Option{highPriorityGroup}, nodeInfos)
	require.NotNil(t, ret)
	assert.True(t, assert.ObjectsAreEqual(*ret, highPriorityGroup))

	// if there's two groups we return the one with higher priority
	ret = e.BestOption([]expander.Option{highPriorityGroup, lowPriorityGroup}, nodeInfos)
	require.NotNil(t, ret)
	assert.True(t, assert.ObjectsAreEqual(*ret, highPriorityGroup))

	// if there's the same two groups in different order the result is the same
	ret = e.BestOption([]expander.Option{lowPriorityGroup, highPriorityGroup}, nodeInfos)
	require.NotNil(t, ret)
	assert.True(t, assert.ObjectsAreEqual(*ret, highPriorityGroup))

	// if there's many different priorities we return the pool with the highest
	ret = e.BestOption([]expander.Option{highPriorityGroup, veryHighPriorityGroup, lowPriorityGroup}, nodeInfos)
	require.NotNil(t, ret)
	assert.True(t, assert.ObjectsAreEqual(*ret, veryHighPriorityGroup))

	// if there's multiple groups with same priority we return either one
	ret = e.BestOption([]expander.Option{highPriorityGroup, veryHighPriorityGroup, veryHighPriorityGroup2}, nodeInfos)
	require.NotNil(t, ret)
	assert.True(t, assert.ObjectsAreEqual(*ret, veryHighPriorityGroup) || assert.ObjectsAreEqual(*ret, veryHighPriorityGroup2))

	// if there's a group with no priority it's assumed to be 0.
	ret = e.BestOption([]expander.Option{lowPriorityGroup, noPriorityGroup}, nodeInfos)
	require.NotNil(t, ret)
	assert.True(t, assert.ObjectsAreEqual(*ret, lowPriorityGroup))

	// if there's a group with no priority and it's the highest it gets picked.
	ret = e.BestOption([]expander.Option{negativePriorityGroup, noPriorityGroup}, nodeInfos)
	require.NotNil(t, ret)
	assert.True(t, assert.ObjectsAreEqual(*ret, noPriorityGroup))
}

func intPtr(v int64) *int64 {
	return &v
}
