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

package groupsize

import (
	"github.com/golang/glog"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/expander"
	"k8s.io/autoscaler/cluster-autoscaler/expander/random"
	schedulercache "k8s.io/kubernetes/pkg/scheduler/cache"
	"sort"
)

const (
	idealSize = 21
)

type groupSize struct {
	fallback expander.Strategy
}

// NewStrategy returns a strategy which picks the nodegroup which is most suitable based on the number scheduled pods
func NewStrategy() expander.Strategy {
	return &groupSize{fallback: random.NewStrategy()}
}

func nodeCpu(nodeInfo *schedulercache.NodeInfo) int64 {
	allocatableCPU := nodeInfo.AllocatableResource().MilliCPU
	consumedPods := sumCPU(nodeInfo.Pods())
	return allocatableCPU - consumedPods
}

func nodeMemory(nodeInfo *schedulercache.NodeInfo) int64 {
	return nodeInfo.AllocatableResource().Memory - sumMemory(nodeInfo.Pods())
}

func optionLess(first *schedulercache.NodeInfo, second *schedulercache.NodeInfo) bool {
	firstCpu := nodeCpu(first)
	secondCpu := nodeCpu(second)
	if firstCpu < secondCpu {
		return true
	}
	if firstCpu == secondCpu && nodeMemory(first) < nodeMemory(second) {
		return true
	}
	return false
}

func (g *groupSize) BestOption(options []expander.Option, nodeInfo map[string]*schedulercache.NodeInfo) *expander.Option {
	allScheduledPods := allScheduledPods(options)
	totalMemory := sumMemory(allScheduledPods)
	totalCPU := sumCPU(allScheduledPods)
	idealCpu := float64(totalCPU) / idealSize
	idealMemory := totalMemory / idealSize
	glog.V(1).Infof("Total Memory: %d", totalMemory)
	glog.V(1).Infof("Total CPU: %d", totalCPU)
	glog.V(1).Infof("Ideal Memory: %d", idealMemory)
	glog.V(1).Infof("Ideal CPU: %f", idealCpu)
	sort.SliceStable(options, func(i, j int) bool {
		return optionLess(nodeInfo[options[i].NodeGroup.Id()], nodeInfo[options[j].NodeGroup.Id()])
	})
	for _, o := range options {
		info := nodeInfo[o.NodeGroup.Id()]
		allocatableCpu := float64(nodeCpu(info))
		allocatableMemory := nodeMemory(info)
		if allocatableCpu > idealCpu && allocatableMemory > idealMemory {
			return &o
		}
	}
	return &options[len(options)-1]
}

func sumMemory(pods []*apiv1.Pod) int64 {
	var sum int64
	for _, p := range pods {
		for _, c := range p.Spec.Containers {
			if c.Resources.Requests.Memory() != nil {
				sum += c.Resources.Requests.Memory().Value()
			}
		}
	}
	return sum
}

func sumCPU(pods []*apiv1.Pod) int64 {
	var sum int64
	for _, p := range pods {
		for _, c := range p.Spec.Containers {
			if c.Resources.Requests.Cpu() != nil {
				sum += c.Resources.Requests.Cpu().MilliValue()
			}
		}
	}
	return sum
}

func allScheduledPods(options []expander.Option) []*apiv1.Pod {
	pods := make([]*apiv1.Pod, 0)
	for _, o := range options {
		pods = append(pods, o.ScheduledPods...)
	}
	return pods
}
