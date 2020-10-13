/*
Copyright 2018 The Kubernetes Authors.

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

package backoff

import (
	"time"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	schedulernodeinfo "k8s.io/kubernetes/pkg/scheduler/nodeinfo"
)

type infiniteBackoff struct {
	backedOff map[string]bool
}

// NewInfiniteBackoff creates a Backoff with infinite duration
func NewInfiniteBackoff() Backoff {
	return &infiniteBackoff{
		backedOff: make(map[string]bool),
	}
}

func (b *infiniteBackoff) Backoff(nodeGroup cloudprovider.NodeGroup, nodeInfo *schedulernodeinfo.NodeInfo, errorClass cloudprovider.InstanceErrorClass, errorCode string, currentTime time.Time) time.Time {
	b.backedOff[nodeGroup.Id()] = true
	return time.Date(9999, 12, 31, 0, 0, 0, 0, time.UTC)
}

func (b *infiniteBackoff) IsBackedOff(nodeGroup cloudprovider.NodeGroup, nodeInfo *schedulernodeinfo.NodeInfo, currentTime time.Time) bool {
	return b.backedOff[nodeGroup.Id()]
}

func (b *infiniteBackoff) RemoveBackoff(nodeGroup cloudprovider.NodeGroup, nodeInfo *schedulernodeinfo.NodeInfo) {
	delete(b.backedOff, nodeGroup.Id())
}

func (b *infiniteBackoff) RemoveStaleBackoffData(currentTime time.Time) {
}
