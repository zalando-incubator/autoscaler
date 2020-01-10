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

package aws

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/autoscaling"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/utils/gpu"
	kubeletapis "k8s.io/kubernetes/pkg/kubelet/apis"
)

func TestBuildGenericLabels(t *testing.T) {
	labels := buildGenericLabels(&asgTemplate{
		AvailableResources: []*instanceResourceInfo{{InstanceType: "c4.large"}},
		Region:             "us-east-1",
	}, "sillyname")
	assert.Equal(t, "us-east-1", labels[kubeletapis.LabelZoneRegion])
	assert.Equal(t, "sillyname", labels[kubeletapis.LabelHostname])
	assert.Equal(t, "c4.large", labels[kubeletapis.LabelInstanceType])
	assert.Equal(t, cloudprovider.DefaultArch, labels[kubeletapis.LabelArch])
	assert.Equal(t, cloudprovider.DefaultOS, labels[kubeletapis.LabelOS])
}

func TestExtractAllocatableResourcesFromAsg(t *testing.T) {
	tags := []*autoscaling.TagDescription{
		{
			Key:   aws.String("k8s.io/cluster-autoscaler/node-template/resources/cpu"),
			Value: aws.String("100m"),
		},
		{
			Key:   aws.String("k8s.io/cluster-autoscaler/node-template/resources/memory"),
			Value: aws.String("100M"),
		},
		{
			Key:   aws.String("k8s.io/cluster-autoscaler/node-template/resources/ephemeral-storage"),
			Value: aws.String("20G"),
		},
	}

	labels := extractAllocatableResourcesFromAsg(tags)

	assert.Equal(t, resource.NewMilliQuantity(100, resource.DecimalSI).String(), labels["cpu"].String())
	expectedMemory := resource.MustParse("100M")
	assert.Equal(t, (&expectedMemory).String(), labels["memory"].String())
	expectedEphemeralStorage := resource.MustParse("20G")
	assert.Equal(t, (&expectedEphemeralStorage).String(), labels["ephemeral-storage"].String())
}

func TestExtractLabelsFromAsg(t *testing.T) {
	tags := []*autoscaling.TagDescription{
		{
			Key:   aws.String("k8s.io/cluster-autoscaler/node-template/label/foo"),
			Value: aws.String("bar"),
		},
		{
			Key:   aws.String("bar"),
			Value: aws.String("baz"),
		},
	}

	labels := extractLabelsFromAsg(tags)

	assert.Equal(t, 1, len(labels))
	assert.Equal(t, "bar", labels["foo"])
}

func TestExtractTaintsFromAsg(t *testing.T) {
	tags := []*autoscaling.TagDescription{
		{
			Key:   aws.String("k8s.io/cluster-autoscaler/node-template/taint/dedicated"),
			Value: aws.String("foo:NoSchedule"),
		},
		{
			Key:   aws.String("k8s.io/cluster-autoscaler/node-template/taint/group"),
			Value: aws.String("bar:NoExecute"),
		},
		{
			Key:   aws.String("k8s.io/cluster-autoscaler/node-template/taint/app"),
			Value: aws.String("fizz:PreferNoSchedule"),
		},
		{
			Key:   aws.String("bar"),
			Value: aws.String("baz"),
		},
		{
			Key:   aws.String("k8s.io/cluster-autoscaler/node-template/taint/blank"),
			Value: aws.String(""),
		},
		{
			Key:   aws.String("k8s.io/cluster-autoscaler/node-template/taint/nosplit"),
			Value: aws.String("some_value"),
		},
	}

	expectedTaints := []apiv1.Taint{
		{
			Key:    "dedicated",
			Value:  "foo",
			Effect: apiv1.TaintEffectNoSchedule,
		},
		{
			Key:    "group",
			Value:  "bar",
			Effect: apiv1.TaintEffectNoExecute,
		},
		{
			Key:    "app",
			Value:  "fizz",
			Effect: apiv1.TaintEffectPreferNoSchedule,
		},
	}

	taints := extractTaintsFromAsg(tags)
	assert.Equal(t, 3, len(taints))
	assert.Equal(t, makeTaintSet(expectedTaints), makeTaintSet(taints))
}

func makeTaintSet(taints []apiv1.Taint) map[apiv1.Taint]bool {
	set := make(map[apiv1.Taint]bool)
	for _, taint := range taints {
		set[taint] = true
	}
	return set
}

func TestFetchExplicitAsgs(t *testing.T) {
	min, max, groupname := 1, 10, "coolasg"

	s := &AutoScalingMock{}
	s.On("DescribeAutoScalingGroups", &autoscaling.DescribeAutoScalingGroupsInput{
		AutoScalingGroupNames: []*string{aws.String(groupname)},
		MaxRecords:            aws.Int64(1),
	}).Return(&autoscaling.DescribeAutoScalingGroupsOutput{
		AutoScalingGroups: []*autoscaling.Group{
			{AutoScalingGroupName: aws.String(groupname)},
		},
	})

	s.On("DescribeAutoScalingGroupsPages",
		&autoscaling.DescribeAutoScalingGroupsInput{
			AutoScalingGroupNames: aws.StringSlice([]string{groupname}),
			MaxRecords:            aws.Int64(maxRecordsReturnedByAPI),
		},
		mock.AnythingOfType("func(*autoscaling.DescribeAutoScalingGroupsOutput, bool) bool"),
	).Run(func(args mock.Arguments) {
		fn := args.Get(1).(func(*autoscaling.DescribeAutoScalingGroupsOutput, bool) bool)
		fn(&autoscaling.DescribeAutoScalingGroupsOutput{
			AutoScalingGroups: []*autoscaling.Group{
				{AutoScalingGroupName: aws.String(groupname)},
			}}, false)
	}).Return(nil)

	do := cloudprovider.NodeGroupDiscoveryOptions{
		// Register the same node group twice with different max nodes.
		// The intention is to test that the asgs.Register method will update
		// the node group instead of registering it twice.
		NodeGroupSpecs: []string{
			fmt.Sprintf("%d:%d:%s", min, max, groupname),
			fmt.Sprintf("%d:%d:%s", min, max-1, groupname),
		},
	}
	// fetchExplicitASGs is called at manager creation time.
	m, err := createAWSManagerInternal(nil, do, &autoScalingWrapper{s}, nil)
	assert.NoError(t, err)

	asgs := m.asgCache.Get()
	assert.Equal(t, 1, len(asgs))
	validateAsg(t, asgs[0], groupname, min, max)
}

func TestBuildInstanceTypes(t *testing.T) {
	ltName, ltVersion, instanceType := "launcher", "1", []string{"t2.large"}

	s := &EC2Mock{}
	s.On("DescribeLaunchTemplateVersions", &ec2.DescribeLaunchTemplateVersionsInput{
		LaunchTemplateName: aws.String(ltName),
		Versions:           []*string{aws.String(ltVersion)},
	}).Return(&ec2.DescribeLaunchTemplateVersionsOutput{
		LaunchTemplateVersions: []*ec2.LaunchTemplateVersion{
			{
				LaunchTemplateData: &ec2.ResponseLaunchTemplateData{
					InstanceType: aws.String(instanceType[0]),
				},
			},
		},
	})

	m, err := createAWSManagerInternal(nil, cloudprovider.NodeGroupDiscoveryOptions{}, nil, &ec2Wrapper{s})
	assert.NoError(t, err)

	asg := asg{
		LaunchTemplateName:    ltName,
		LaunchTemplateVersion: ltVersion,
	}

	builtInstanceType, err := m.buildInstanceTypes(&asg)

	assert.NoError(t, err)
	assert.Equal(t, instanceType, builtInstanceType)
}

func TestFetchAutoAsgs(t *testing.T) {
	min, max := 1, 10
	groupname, tags := "coolasg", []string{"tag", "anothertag"}

	s := &AutoScalingMock{}
	// Lookup groups associated with tags
	expectedTagsInput := &autoscaling.DescribeTagsInput{
		Filters: []*autoscaling.Filter{
			{Name: aws.String("key"), Values: aws.StringSlice([]string{tags[0]})},
			{Name: aws.String("key"), Values: aws.StringSlice([]string{tags[1]})},
		},
		MaxRecords: aws.Int64(maxRecordsReturnedByAPI),
	}
	// Use MatchedBy pattern to avoid list order issue https://github.com/kubernetes/autoscaler/issues/1346
	s.On("DescribeTagsPages", mock.MatchedBy(tagsMatcher(expectedTagsInput)),
		mock.AnythingOfType("func(*autoscaling.DescribeTagsOutput, bool) bool"),
	).Run(func(args mock.Arguments) {
		fn := args.Get(1).(func(*autoscaling.DescribeTagsOutput, bool) bool)
		fn(&autoscaling.DescribeTagsOutput{
			Tags: []*autoscaling.TagDescription{
				{ResourceId: aws.String(groupname)},
				{ResourceId: aws.String(groupname)},
			}}, false)
	}).Return(nil).Once()

	// Describe the group to register it, then again to generate the instance
	// cache.
	s.On("DescribeAutoScalingGroupsPages",
		&autoscaling.DescribeAutoScalingGroupsInput{
			AutoScalingGroupNames: aws.StringSlice([]string{groupname}),
			MaxRecords:            aws.Int64(maxRecordsReturnedByAPI),
		},
		mock.AnythingOfType("func(*autoscaling.DescribeAutoScalingGroupsOutput, bool) bool"),
	).Run(func(args mock.Arguments) {
		fn := args.Get(1).(func(*autoscaling.DescribeAutoScalingGroupsOutput, bool) bool)
		fn(&autoscaling.DescribeAutoScalingGroupsOutput{
			AutoScalingGroups: []*autoscaling.Group{{
				AutoScalingGroupName: aws.String(groupname),
				MinSize:              aws.Int64(int64(min)),
				MaxSize:              aws.Int64(int64(max)),
			}}}, false)
	}).Return(nil).Twice()

	do := cloudprovider.NodeGroupDiscoveryOptions{
		NodeGroupAutoDiscoverySpecs: []string{fmt.Sprintf("asg:tag=%s", strings.Join(tags, ","))},
	}

	// fetchAutoASGs is called at manager creation time, via forceRefresh
	m, err := createAWSManagerInternal(nil, do, &autoScalingWrapper{s}, nil)
	assert.NoError(t, err)

	asgs := m.asgCache.Get()
	assert.Equal(t, 1, len(asgs))
	validateAsg(t, asgs[0], groupname, min, max)

	// Simulate the previously discovered ASG disappearing
	s.On("DescribeTagsPages", mock.MatchedBy(tagsMatcher(expectedTagsInput)),
		mock.AnythingOfType("func(*autoscaling.DescribeTagsOutput, bool) bool"),
	).Run(func(args mock.Arguments) {
		fn := args.Get(1).(func(*autoscaling.DescribeTagsOutput, bool) bool)
		fn(&autoscaling.DescribeTagsOutput{Tags: []*autoscaling.TagDescription{}}, false)
	}).Return(nil).Once()

	err = m.asgCache.regenerate()
	assert.NoError(t, err)
	assert.Empty(t, m.asgCache.Get())
}

func TestTemplateNodes(t *testing.T) {
	m, err := createAWSManagerInternal(nil, cloudprovider.NodeGroupDiscoveryOptions{}, nil, nil)
	assert.NoError(t, err)

	// No data and no nodes, use raw AWS data
	resources, err := m.availableResources("p2.xlarge")
	assert.NoError(t, err)
	assertResourcesEqual(t, parseResourceList(map[apiv1.ResourceName]string{
		apiv1.ResourceCPU:     "4",
		apiv1.ResourceMemory:  "62464Mi",
		apiv1.ResourcePods:    "110",
		gpu.ResourceNvidiaGPU: "1",
	}), resources.Capacity)
	assertResourcesEqual(t, parseResourceList(map[apiv1.ResourceName]string{
		apiv1.ResourceCPU:     "4",
		apiv1.ResourceMemory:  "62464Mi",
		apiv1.ResourcePods:    "110",
		gpu.ResourceNvidiaGPU: "1",
	}), resources.Allocatable)

	// Update the manager with some nodes
	m.updateAvailableResources([]*apiv1.Node{
		sampleNode(
			"p2-xlarge-1", "p2.xlarge",
			parseResourceList(map[apiv1.ResourceName]string{
				apiv1.ResourceCPU:     "4",
				apiv1.ResourceMemory:  "62000Mi",
				apiv1.ResourcePods:    "100",
				gpu.ResourceNvidiaGPU: "1",
			}),
			parseResourceList(map[apiv1.ResourceName]string{
				apiv1.ResourceCPU:     "3700m",
				apiv1.ResourceMemory:  "61800Mi",
				apiv1.ResourcePods:    "100",
				gpu.ResourceNvidiaGPU: "1",
			})),
		sampleNode(
			"p2-xlarge-2", "p2.xlarge",
			parseResourceList(map[apiv1.ResourceName]string{
				apiv1.ResourceCPU:     "4",
				apiv1.ResourceMemory:  "61700Mi",
				apiv1.ResourcePods:    "99",
				gpu.ResourceNvidiaGPU: "1",
			}),
			parseResourceList(map[apiv1.ResourceName]string{
				apiv1.ResourceCPU:     "3800m",
				apiv1.ResourceMemory:  "61600Mi",
				apiv1.ResourcePods:    "99",
				gpu.ResourceNvidiaGPU: "1",
			})),
	})

	// Information for p2.xlarge should now be based on existing nodes
	expectedCapacity := parseResourceList(map[apiv1.ResourceName]string{
		apiv1.ResourceCPU:     "4",
		apiv1.ResourceMemory:  "61700Mi",
		apiv1.ResourcePods:    "99",
		gpu.ResourceNvidiaGPU: "1",
	})
	expectedAllocatable := parseResourceList(map[apiv1.ResourceName]string{
		apiv1.ResourceCPU:     "3700m",
		apiv1.ResourceMemory:  "61600Mi",
		apiv1.ResourcePods:    "99",
		gpu.ResourceNvidiaGPU: "1",
	})

	updatedResources, err := m.availableResources("p2.xlarge")
	assert.NoError(t, err)
	assertResourcesEqual(t, expectedCapacity, updatedResources.Capacity)
	assertResourcesEqual(t, expectedAllocatable, updatedResources.Allocatable)

	// Allocatable information for other instance types should include data from the existing nodes
	otherTypeResources, err := m.availableResources("p2.16xlarge")
	assert.NoError(t, err)
	assertResourcesEqual(t, parseResourceList(map[apiv1.ResourceName]string{
		apiv1.ResourceCPU:     "64",
		apiv1.ResourceMemory:  "768Gi",
		apiv1.ResourcePods:    "110",
		gpu.ResourceNvidiaGPU: "16",
	}), otherTypeResources.Capacity)
	assertResourcesEqual(t, parseResourceList(map[apiv1.ResourceName]string{
		apiv1.ResourceCPU:     "63700m",   // 64 - 300m
		apiv1.ResourceMemory:  "786232Mi", // 768Gi - 200Mi
		apiv1.ResourcePods:    "110",
		gpu.ResourceNvidiaGPU: "16",
	}), otherTypeResources.Allocatable)
}

func assertResourcesEqual(t *testing.T, expected, actual apiv1.ResourceList) {
	for resourceName, resourceValue := range expected {
		actualValue, ok := actual[resourceName]
		assert.True(t, ok, "expected resource %s to be %s, found none", resourceName, &resourceValue)
		if ok {
			assert.True(t, resourceValue.Cmp(actualValue) == 0, "expected resource %s to be %s, found %s", resourceName, &resourceValue, &actualValue)
		}
	}

	for resourceName := range actual {
		_, ok := expected[resourceName]
		assert.True(t, ok, "found unexpected value for resource %s", resourceName)
	}
}

func parseResourceList(from map[apiv1.ResourceName]string) apiv1.ResourceList {
	result := make(apiv1.ResourceList, len(from))
	for resourceName, value := range from {
		result[resourceName] = resource.MustParse(value)
	}
	return result
}

func sampleNode(nodeName string, instanceType string, capacity, allocatable apiv1.ResourceList) *apiv1.Node {
	return &apiv1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   nodeName,
			Labels: map[string]string{kubeletapis.LabelInstanceType: instanceType},
		},
		Status: apiv1.NodeStatus{
			Capacity:    capacity,
			Allocatable: allocatable,
		},
	}
}

func tagsMatcher(expected *autoscaling.DescribeTagsInput) func(*autoscaling.DescribeTagsInput) bool {
	return func(actual *autoscaling.DescribeTagsInput) bool {
		expectedTags := flatTagSlice(expected.Filters)
		actualTags := flatTagSlice(actual.Filters)

		return *expected.MaxRecords == *actual.MaxRecords && reflect.DeepEqual(expectedTags, actualTags)
	}
}

func flatTagSlice(filters []*autoscaling.Filter) []string {
	tags := []string{}
	for _, filter := range filters {
		tags = append(tags, aws.StringValueSlice(filter.Values)...)
	}
	// Sort slice for compare
	sort.Strings(tags)
	return tags
}
