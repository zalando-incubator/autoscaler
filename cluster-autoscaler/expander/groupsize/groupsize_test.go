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
	"fmt"
	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/expander"
	schedulercache "k8s.io/kubernetes/pkg/scheduler/cache"
	"math"
	"testing"
)

type fakeNodeGroup struct {
	id int
}

func (f fakeNodeGroup) MaxSize() int {
	panic("unimplemented")
}

func (f fakeNodeGroup) MinSize() int {
	panic("unimplemented")
}

func (f fakeNodeGroup) TargetSize() (int, error) {
	panic("unimplemented")
}

func (f fakeNodeGroup) IncreaseSize(delta int) error {
	panic("unimplemented")
}

func (f fakeNodeGroup) DeleteNodes(nodes []*apiv1.Node) error {
	panic("unimplemented")
}

func (f fakeNodeGroup) DecreaseTargetSize(delta int) error {
	panic("unimplemented")
}

func (f fakeNodeGroup) Id() string {
	return fmt.Sprint(f.id)
}

func (f fakeNodeGroup) Debug() string {
	panic("unimplemented")
}

func (f fakeNodeGroup) Nodes() ([]string, error) {
	panic("unimplemented")
}

func (f fakeNodeGroup) TemplateNodeInfo() (*schedulercache.NodeInfo, error) {
	panic("unimplemented")
}

func (f fakeNodeGroup) Exist() bool {
	panic("unimplemented")
}

func (f fakeNodeGroup) Create() (cloudprovider.NodeGroup, error) {
	panic("unimplemented")
}

func (f fakeNodeGroup) Delete() error {
	panic("unimplemented")
}

func (f fakeNodeGroup) Autoprovisioned() bool {
	panic("unimplemented")
}

func makeNodeGroup(id int) cloudprovider.NodeGroup {
	return &fakeNodeGroup{id: id}
}

func TestGroupSize(t *testing.T) {
	for _, tc := range []struct {
		numOptions     int
		numPods        int
		expectedOption int
	}{
		{
			numOptions:     20,
			numPods:        2,
			expectedOption: 1,
		},
		{
			numOptions:     20,
			numPods:        20,
			expectedOption: 1,
		},
		{
			numOptions:     20,
			numPods:        40,
			expectedOption: 2,
		},
		{
			numOptions:     20,
			numPods:        60,
			expectedOption: 3,
		},
		{
			numOptions:     20,
			numPods:        80,
			expectedOption: 3,
		},
		{
			numOptions:     20,
			numPods:        200,
			expectedOption: 5,
		},
		{
			numOptions:     3,
			numPods:        200,
			expectedOption: 3,
		},
	} {
		t.Run(fmt.Sprintf("expected option %d", tc.expectedOption), func(t *testing.T) {
			strategy := NewStrategy()
			option := strategy.BestOption(makeOptions(tc.numOptions, tc.numPods), makeNodeInfos(tc.numOptions))
			assert.Equal(t, fmt.Sprint(tc.expectedOption), option.NodeGroup.Id())
		})
	}
}

func makeNodeInfos(count int) map[string]*schedulercache.NodeInfo {
	infos := make(map[string]*schedulercache.NodeInfo)
	for i := 0; i < count; i++ {
		infos[fmt.Sprint(i+1)] = makeTestNodeInfo(i)
	}
	return infos
}

func makeOptions(numOptions int, numPods int) []expander.Option {
	options := make([]expander.Option, numOptions)
	podsPerOption := numPods / numOptions
	for i := 0; i < numOptions; i++ {
		options[i] = expander.Option{
			NodeGroup:     makeNodeGroup(i + 1),
			NodeCount:     0,
			Pods:          makeTestPods(1),
			ScheduledPods: makeTestPods(podsPerOption),
		}
	}
	return options
}

func makeTestNodeInfo(size int) *schedulercache.NodeInfo {
	info := schedulercache.NewNodeInfo()
	nodeResource := schedulercache.NewResource(apiv1.ResourceList{
		apiv1.ResourceMemory: resource.MustParse(fmt.Sprintf("%dGi", int(math.Pow(2, float64(size))*8))),
		apiv1.ResourceCPU:    resource.MustParse(fmt.Sprintf("%d", int(math.Pow(2, float64(size))))),
	})
	info.SetAllocatableResource(nodeResource)
	return info
}

func makeTestPods(count int) []*apiv1.Pod {
	pods := make([]*apiv1.Pod, count)
	for i := 0; i < count; i++ {
		pod := &apiv1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("test-pod-%d", i)},
			Spec: apiv1.PodSpec{
				Containers: []apiv1.Container{
					{
						Resources: apiv1.ResourceRequirements{
							Limits: apiv1.ResourceList{
								apiv1.ResourceCPU:    resource.MustParse("1"),
								apiv1.ResourceMemory: resource.MustParse("1Gi"),
							},
							Requests: apiv1.ResourceList{
								apiv1.ResourceCPU:    resource.MustParse("1"),
								apiv1.ResourceMemory: resource.MustParse("1Gi"),
							},
						},
					},
				},
			},
		}
		pods[i] = pod
	}
	return pods
}
