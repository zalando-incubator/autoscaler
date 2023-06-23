/*
Copyright 2017 The Kubernetes Authors.

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

package scheduler

import (
	"strings"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	schedulerframework "k8s.io/kubernetes/pkg/scheduler/framework"
)

// CreateNodeNameToInfoMap obtains a list of pods and pivots that list into a map where the keys are node names
// and the values are the aggregated information for that node. Pods waiting lower priority pods preemption
// (pod.Status.NominatedNodeName is set) are also added to list of pods for a node.
func CreateNodeNameToInfoMap(pods []*apiv1.Pod, nodes []*apiv1.Node) map[string]*schedulerframework.NodeInfo {
	nodeNameToNodeInfo := make(map[string]*schedulerframework.NodeInfo)
	for _, pod := range pods {
		nodeName := pod.Spec.NodeName
		if nodeName == "" {
			nodeName = pod.Status.NominatedNodeName
		}
		if _, ok := nodeNameToNodeInfo[nodeName]; !ok {
			nodeNameToNodeInfo[nodeName] = schedulerframework.NewNodeInfo()
		}
		nodeNameToNodeInfo[nodeName].AddPod(pod)
	}

	for _, node := range nodes {
		if _, ok := nodeNameToNodeInfo[node.Name]; !ok {
			nodeNameToNodeInfo[node.Name] = schedulerframework.NewNodeInfo()
		}
		nodeNameToNodeInfo[node.Name].SetNode(node)
	}

	// Some pods may be out of sync with node lists. Removing incomplete node infos.
	keysToRemove := make([]string, 0)
	for key, nodeInfo := range nodeNameToNodeInfo {
		if nodeInfo.Node() == nil {
			keysToRemove = append(keysToRemove, key)
		}
	}
	for _, key := range keysToRemove {
		delete(nodeNameToNodeInfo, key)
	}

	return nodeNameToNodeInfo
}

// CloneNodeInfo copies the provided schedulernodeinfo.NodeInfo __correctly__. The upstream version doesn't deep-copy
// the underlying Node object, leading to potential corruption if someone modifies the Node directly. Note that it still
// doesn't deep-copy the pods, but this should hopefully not create any issues.
func CloneNodeInfo(node *schedulerframework.NodeInfo) *schedulerframework.NodeInfo {
	copyNode := node.Node().DeepCopy()
	nodeInfo := schedulerframework.NewNodeInfo()
	nodeInfo.SetNode(copyNode)
	return nodeInfo
}

func isHugePageResourceName(name apiv1.ResourceName) bool {
	return strings.HasPrefix(string(name), apiv1.ResourceHugePagesPrefix)
}

// ResourceToResourceList returns a resource list of the resource.
func ResourceToResourceList(r *schedulerframework.Resource) apiv1.ResourceList {
	result := apiv1.ResourceList{
		apiv1.ResourceCPU:              *resource.NewMilliQuantity(r.MilliCPU, resource.DecimalSI),
		apiv1.ResourceMemory:           *resource.NewQuantity(r.Memory, resource.BinarySI),
		apiv1.ResourcePods:             *resource.NewQuantity(int64(r.AllowedPodNumber), resource.BinarySI),
		apiv1.ResourceEphemeralStorage: *resource.NewQuantity(r.EphemeralStorage, resource.BinarySI),
	}
	for rName, rQuant := range r.ScalarResources {
		if isHugePageResourceName(rName) {
			result[rName] = *resource.NewQuantity(rQuant, resource.BinarySI)
		} else {
			result[rName] = *resource.NewQuantity(rQuant, resource.DecimalSI)
		}
	}
	return result
}
