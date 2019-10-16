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

package daemonset

import (
	"strings"
	"testing"
	"time"

	"k8s.io/autoscaler/cluster-autoscaler/simulator"
	. "k8s.io/autoscaler/cluster-autoscaler/utils/test"

	appsv1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	schedulernodeinfo "k8s.io/kubernetes/pkg/scheduler/nodeinfo"

	"github.com/stretchr/testify/assert"
)

var (
	exampleCPURequests    = resource.MustParse("25m")
	exampleMemoryRequests = resource.MustParse("100Mi")
	exampleCPULimits      = resource.MustParse("50m")
	exampleMemoryLimits   = resource.MustParse("250Mi")
)

func TestGetDaemonSetPodsForNode(t *testing.T) {
	node := BuildTestNode("node", 1000, 1073741824)
	SetNodeReadyState(node, true, time.Now())
	nodeInfo := schedulernodeinfo.NewNodeInfo()
	nodeInfo.SetNode(node)

	predicateChecker := simulator.NewTestPredicateChecker()
	ds1 := newDaemonSet("ds1")
	ds2 := newDaemonSet("ds2")
	ds2.Spec.Template.Spec.NodeSelector = map[string]string{"foo": "bar"}

	pods := GetDaemonSetPodsForNode(nodeInfo, []*appsv1.DaemonSet{ds1, ds2}, predicateChecker)

	assert.Equal(t, 1, len(pods))
	dsPod := pods[0]
	assert.True(t, strings.HasPrefix(dsPod.Name, "ds1"))

	containerWithRequests := dsPod.Spec.Containers[0]
	assert.Equal(t, exampleCPURequests, containerWithRequests.Resources.Requests[apiv1.ResourceCPU])
	assert.Equal(t, exampleMemoryRequests, containerWithRequests.Resources.Requests[apiv1.ResourceMemory])
	assert.Equal(t, exampleCPULimits, containerWithRequests.Resources.Limits[apiv1.ResourceCPU])
	assert.Equal(t, exampleMemoryLimits, containerWithRequests.Resources.Limits[apiv1.ResourceMemory])

	containerWithOnlyLimits := dsPod.Spec.Containers[1]
	assert.Equal(t, exampleCPULimits, containerWithOnlyLimits.Resources.Requests[apiv1.ResourceCPU])
	assert.Equal(t, exampleMemoryLimits, containerWithOnlyLimits.Resources.Requests[apiv1.ResourceMemory])
	assert.Equal(t, exampleCPULimits, containerWithOnlyLimits.Resources.Limits[apiv1.ResourceCPU])
	assert.Equal(t, exampleMemoryLimits, containerWithOnlyLimits.Resources.Limits[apiv1.ResourceMemory])

	assert.Equal(t, 1, len(GetDaemonSetPodsForNode(nodeInfo, []*appsv1.DaemonSet{ds1}, predicateChecker)))
	assert.Equal(t, 0, len(GetDaemonSetPodsForNode(nodeInfo, []*appsv1.DaemonSet{ds2}, predicateChecker)))
	assert.Equal(t, 0, len(GetDaemonSetPodsForNode(nodeInfo, []*appsv1.DaemonSet{}, predicateChecker)))
}

func newDaemonSet(name string) *appsv1.DaemonSet {
	return &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: metav1.NamespaceDefault,
		},
		Spec: appsv1.DaemonSetSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"name": "simple-daemon", "type": "production"}},
			Template: apiv1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"name": "simple-daemon", "type": "production"},
				},
				Spec: apiv1.PodSpec{
					Containers: []apiv1.Container{
						{
							Name:  "first",
							Image: "foo/bar",
							Resources: apiv1.ResourceRequirements{
								Requests: apiv1.ResourceList{apiv1.ResourceCPU: exampleCPURequests, apiv1.ResourceMemory: exampleMemoryRequests},
								Limits:   apiv1.ResourceList{apiv1.ResourceCPU: exampleCPULimits, apiv1.ResourceMemory: exampleMemoryLimits},
							},
						},
						{
							Name:  "second",
							Image: "foo/baz",
							Resources: apiv1.ResourceRequirements{
								Limits: apiv1.ResourceList{apiv1.ResourceCPU: exampleCPULimits, apiv1.ResourceMemory: exampleMemoryLimits},
							},
						},
					},
				},
			},
		},
	}
}
