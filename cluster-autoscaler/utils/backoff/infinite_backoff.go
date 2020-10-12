package backoff

import (
	"time"

	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	schedulernodeinfo "k8s.io/kubernetes/pkg/scheduler/nodeinfo"
)

type infiniteBackoff struct {
	backedOff map[string]bool
}

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
