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
	apiv1 "k8s.io/api/core/v1"
	schedulernodeinfo "k8s.io/kubernetes/pkg/scheduler/nodeinfo"
)

// CreateNodeNameToInfoMap obtains a list of pods and pivots that list into a map where the keys are node names
// and the values are the aggregated information for that node. Pods waiting lower priority pods preemption
// (pod.Status.NominatedNodeName is set) are also added to list of pods for a node.
func CreateNodeNameToInfoMap(pods []*apiv1.Pod, nodes []*apiv1.Node) map[string]*schedulernodeinfo.NodeInfo {
	nodeNameToNodeInfo := make(map[string]*schedulernodeinfo.NodeInfo)
	for _, pod := range pods {
		nodeName := pod.Spec.NodeName
		if nodeName == "" {
			nodeName = pod.Status.NominatedNodeName
		}
		if _, ok := nodeNameToNodeInfo[nodeName]; !ok {
			nodeNameToNodeInfo[nodeName] = schedulernodeinfo.NewNodeInfo()
		}
		nodeNameToNodeInfo[nodeName].AddPod(pod)
	}

	for _, node := range nodes {
		if _, ok := nodeNameToNodeInfo[node.Name]; !ok {
			nodeNameToNodeInfo[node.Name] = schedulernodeinfo.NewNodeInfo()
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
func CloneNodeInfo(nodeInfo *schedulernodeinfo.NodeInfo) *schedulernodeinfo.NodeInfo {
	result := schedulernodeinfo.NewNodeInfo(nodeInfo.Pods()...)
	// I have no idea why this can return an error (it doesn't even do this in the code). Multiple other places just
	// ignore it, and I'm not sure if I want to modify the function signatures to propagate the error correctly. Let's
	// just panic for now, because I'm not comfortable with ignoring it.
	err := result.SetNode(nodeInfo.Node().DeepCopy())
	if err != nil {
		panic(err)
	}
	return result
}
