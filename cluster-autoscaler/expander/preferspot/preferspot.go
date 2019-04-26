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
	"github.com/golang/glog"
	"k8s.io/autoscaler/cluster-autoscaler/expander"
	"k8s.io/autoscaler/cluster-autoscaler/expander/random"
	schedulercache "k8s.io/kubernetes/pkg/scheduler/cache"
)

const (
	spotLabel = "aws.amazon.com/spot"
)

type preferspot struct {
	fallback expander.Strategy
}

// NewStrategy returns a scale up strategy (expander) that picks a random node group with spot instances if possible, or
// any random node group otherwise
func NewStrategy() expander.Strategy {
	return &preferspot{
		fallback: random.NewStrategy(),
	}
}

// BestOption selects the expansion option based on whether a spot group exists
func (ps *preferspot) BestOption(options []expander.Option, nodeInfo map[string]*schedulercache.NodeInfo) *expander.Option {
	var spotOptions []expander.Option
	for _, option := range options {
		info, found := nodeInfo[option.NodeGroup.Id()]
		if !found {
			glog.Warningf("No node info for %s", option.NodeGroup.Id())
			continue
		}
		if info.Node().Labels[spotLabel] == "true" {
			spotOptions = append(spotOptions, option)
		}
	}

	// If we can scale up the spot groups, do that. Otherwise, select anything.
	if len(spotOptions) > 0 {
		return ps.fallback.BestOption(spotOptions, nodeInfo)
	}
	return ps.fallback.BestOption(options, nodeInfo)
}
