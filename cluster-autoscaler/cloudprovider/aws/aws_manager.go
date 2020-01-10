/*
Copyright 2016 The Kubernetes Authors.

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

//go:generate go run ec2_instance_types/gen.go

package aws

import (
	"fmt"
	"io"
	"math/rand"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/autoscaling"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/golang/glog"
	"gopkg.in/gcfg.v1"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/utils/gpu"
	provider_aws "k8s.io/kubernetes/pkg/cloudprovider/providers/aws"
	kubeletapis "k8s.io/kubernetes/pkg/kubelet/apis"
)

const (
	operationWaitTimeout    = 5 * time.Second
	operationPollInterval   = 100 * time.Millisecond
	maxRecordsReturnedByAPI = 100
	maxAsgNamesPerDescribe  = 50
	refreshInterval         = 10 * time.Second
	megabyte                = 1024 * 1024
)

// AwsManager is handles aws communication and data caching.
type AwsManager struct {
	autoScalingService autoScalingWrapper
	ec2Service         ec2Wrapper
	asgCache           *asgCache
	lastRefresh        time.Time

	// Node capacity/allocatable by instance type. Currently doesn't expire because it won't grow uncontrollably,
	// it's updated when there are nodes with this size, and the fallback case depends on the data that doesn't change
	// that often.
	instanceResourceCache map[string]*instanceResourceInfo

	// The current maximum of reserved resources (<capacity> - <allocatable>) across the whole cluster. Updated on every
	// Refresh() call
	reservedResources apiv1.ResourceList
}

type instanceResourceInfo struct {
	InstanceType string
	Capacity     apiv1.ResourceList
	Allocatable  apiv1.ResourceList
}

type asgTemplate struct {
	AvailableResources []*instanceResourceInfo
	Region             string
	Zone               string
	Tags               []*autoscaling.TagDescription
}

// createAwsManagerInternal allows for a customer autoScalingWrapper to be passed in by tests
func createAWSManagerInternal(
	configReader io.Reader,
	discoveryOpts cloudprovider.NodeGroupDiscoveryOptions,
	autoScalingService *autoScalingWrapper,
	ec2Service *ec2Wrapper,
) (*AwsManager, error) {
	if configReader != nil {
		var cfg provider_aws.CloudConfig
		if err := gcfg.ReadInto(&cfg, configReader); err != nil {
			glog.Errorf("Couldn't read config: %v", err)
			return nil, err
		}
	}

	if autoScalingService == nil || ec2Service == nil {
		sess := session.New()

		if autoScalingService == nil {
			autoScalingService = &autoScalingWrapper{autoscaling.New(sess)}
		}

		if ec2Service == nil {
			ec2Service = &ec2Wrapper{ec2.New(sess)}
		}
	}

	specs, err := discoveryOpts.ParseASGAutoDiscoverySpecs()
	if err != nil {
		return nil, err
	}

	cache, err := newASGCache(*autoScalingService, discoveryOpts.NodeGroupSpecs, specs)
	if err != nil {
		return nil, err
	}

	manager := &AwsManager{
		autoScalingService:    *autoScalingService,
		ec2Service:            *ec2Service,
		asgCache:              cache,
		instanceResourceCache: make(map[string]*instanceResourceInfo),
		reservedResources:     make(apiv1.ResourceList),
	}

	if err := manager.forceRefresh(); err != nil {
		return nil, err
	}

	return manager, nil
}

// CreateAwsManager constructs awsManager object.
func CreateAwsManager(configReader io.Reader, discoveryOpts cloudprovider.NodeGroupDiscoveryOptions) (*AwsManager, error) {
	return createAWSManagerInternal(configReader, discoveryOpts, nil, nil)
}

// Refresh is called before every main loop and can be used to dynamically update cloud provider state.
// In particular the list of node groups returned by NodeGroups can change as a result of CloudProvider.Refresh().
func (m *AwsManager) Refresh(existingNodes []*apiv1.Node) error {
	m.updateAvailableResources(existingNodes)
	if m.lastRefresh.Add(refreshInterval).After(time.Now()) {
		return nil
	}
	return m.forceRefresh()
}

func (m *AwsManager) forceRefresh() error {
	if err := m.asgCache.regenerate(); err != nil {
		glog.Errorf("Failed to regenerate ASG cache: %v", err)
		return err
	}
	m.lastRefresh = time.Now()
	glog.V(2).Infof("Refreshed ASG list, next refresh after %v", m.lastRefresh.Add(refreshInterval))
	return nil
}

// GetAsgForInstance returns AsgConfig of the given Instance
func (m *AwsManager) GetAsgForInstance(instance AwsInstanceRef) *asg {
	return m.asgCache.FindForInstance(instance)
}

// Cleanup the ASG cache.
func (m *AwsManager) Cleanup() {
	m.asgCache.Cleanup()
}

func (m *AwsManager) getAsgs() []*asg {
	return m.asgCache.Get()
}

// SetAsgSize sets ASG size.
func (m *AwsManager) SetAsgSize(asg *asg, size int) error {
	return m.asgCache.SetAsgSize(asg, size)
}

// DeleteInstances deletes the given instances. All instances must be controlled by the same ASG.
func (m *AwsManager) DeleteInstances(instances []*AwsInstanceRef) error {
	return m.asgCache.DeleteInstances(instances)
}

// GetAsgNodes returns Asg nodes.
func (m *AwsManager) GetAsgNodes(ref AwsRef) ([]AwsInstanceRef, error) {
	return m.asgCache.InstancesByAsg(ref)
}

func (m *AwsManager) getAsgTemplate(asg *asg) (*asgTemplate, error) {
	if len(asg.AvailabilityZones) < 1 {
		return nil, fmt.Errorf("Unable to get first AvailabilityZone for %s", asg.Name)
	}

	az := asg.AvailabilityZones[rand.Intn(len(asg.AvailabilityZones))]
	region := az[0 : len(az)-1]

	if len(asg.AvailabilityZones) > 1 {
		glog.Warningf("Found multiple availability zones, using %s\n", az)
	}

	instanceTypeNames, err := m.buildInstanceTypes(asg)
	if err != nil {
		return nil, err
	}

	var resources []*instanceResourceInfo
	for _, name := range instanceTypeNames {
		instanceResources, err := m.availableResources(name)
		if err != nil {
			return nil, err
		}
		resources = append(resources, instanceResources)
	}

	return &asgTemplate{
		AvailableResources: resources,
		Region:             region,
		Zone:               az,
		Tags:               asg.Tags,
	}, nil
}

func (m *AwsManager) buildInstanceTypes(asg *asg) ([]string, error) {
	if len(asg.InstanceTypeOverrides) > 0 {
		return asg.InstanceTypeOverrides, nil
	}

	if asg.LaunchConfigurationName != "" {
		result, err := m.autoScalingService.getInstanceTypeByLCName(asg.LaunchConfigurationName)
		return []string{result}, err
	} else if asg.LaunchTemplateName != "" && asg.LaunchTemplateVersion != "" {
		result, err := m.ec2Service.getInstanceTypeByLT(asg.LaunchTemplateName, asg.LaunchTemplateVersion)
		return []string{result}, err
	}

	return nil, fmt.Errorf("Unable to get instance type from launch config or launch template")
}

func (m *AwsManager) buildNodeFromTemplate(asg *asg, template *asgTemplate) (*apiv1.Node, error) {
	node := apiv1.Node{}
	nodeName := fmt.Sprintf("%s-asg-%d", asg.Name, rand.Int63())

	node.ObjectMeta = metav1.ObjectMeta{
		Name:     nodeName,
		SelfLink: fmt.Sprintf("/api/v1/nodes/%s", nodeName),
		Labels:   map[string]string{},
	}

	node.Status = apiv1.NodeStatus{
		Capacity:    apiv1.ResourceList{},
		Allocatable: apiv1.ResourceList{},
	}

	for _, instanceResources := range template.AvailableResources {
		for resourceName, capacity := range instanceResources.Capacity {
			updateMinResource(node.Status.Capacity, resourceName, capacity)
		}
		for resourceName, allocatable := range instanceResources.Allocatable {
			updateMinResource(node.Status.Allocatable, resourceName, allocatable)
		}
	}

	resourcesFromTags := extractAllocatableResourcesFromAsg(template.Tags)
	if val, ok := resourcesFromTags["ephemeral-storage"]; ok {
		node.Status.Capacity[apiv1.ResourceEphemeralStorage] = *val
		node.Status.Allocatable[apiv1.ResourceEphemeralStorage] = *val
	}

	// NodeLabels
	node.Labels = cloudprovider.JoinStringMaps(node.Labels, extractLabelsFromAsg(template.Tags))
	// GenericLabels
	node.Labels = cloudprovider.JoinStringMaps(node.Labels, buildGenericLabels(template, nodeName))

	node.Spec.Taints = extractTaintsFromAsg(template.Tags)

	node.Status.Conditions = cloudprovider.BuildReadyConditions()
	return &node, nil
}

// availableResources returns information about node resources (capacity/allocatable) for a particular instance type
func (m *AwsManager) availableResources(instanceType string) (*instanceResourceInfo, error) {
	// The cache either contains data collected from live nodes or we've already constructed something
	// from the AWS information
	if cached, ok := m.instanceResourceCache[instanceType]; ok {
		return cached, nil
	}

	awsTemplate, ok := InstanceTypes[instanceType]
	if !ok {
		return nil, fmt.Errorf("unknown instance type %s", instanceType)
	}

	cpuCapacity := *resource.NewQuantity(awsTemplate.VCPU, resource.DecimalSI)
	memCapacity := *resource.NewQuantity(awsTemplate.MemoryMb*megabyte, resource.DecimalSI)
	gpuCapacity := *resource.NewQuantity(awsTemplate.GPU, resource.DecimalSI)
	// TODO: get a real value.
	podCapacity := *resource.NewQuantity(110, resource.DecimalSI)

	// Build a node from the template. We set the capacity to the static AWS template data,
	// and subtract the maximum reserved CPU/memory across all the nodes in the cluster to get
	// the allocatable value.
	template := &instanceResourceInfo{
		InstanceType: awsTemplate.InstanceType,
		Capacity: apiv1.ResourceList{
			apiv1.ResourceCPU:     cpuCapacity,
			apiv1.ResourceMemory:  memCapacity,
			gpu.ResourceNvidiaGPU: gpuCapacity,
			apiv1.ResourcePods:    podCapacity,
		},
		Allocatable: apiv1.ResourceList{
			apiv1.ResourceCPU:    sub(cpuCapacity, m.reservedResources[apiv1.ResourceCPU]),
			apiv1.ResourceMemory: sub(memCapacity, m.reservedResources[apiv1.ResourceMemory]),
			// No reservations for GPU/Pods
			gpu.ResourceNvidiaGPU: gpuCapacity,
			apiv1.ResourcePods:    podCapacity,
		},
	}
	m.instanceResourceCache[instanceType] = template
	return template, nil
}

// updateAvailableResources collects information about resource availability from existing nodes and updates
// the cache and the reservation information
func (m *AwsManager) updateAvailableResources(nodes []*apiv1.Node) {
	globalReserved := make(apiv1.ResourceList)
	instanceResources := make(map[string]*instanceResourceInfo)

	// Collect information from the existing nodes in the cluster. We take a minimum of
	// all resources for every resource when we compute capacity and allocatable, and a
	// global maximum across all nodes when we compute resource reservations
	for _, node := range nodes {
		instanceType, ok := node.GetLabels()[kubeletapis.LabelInstanceType]
		if !ok {
			continue
		}

		resources, ok := instanceResources[instanceType]
		if !ok {
			resources = &instanceResourceInfo{
				InstanceType: instanceType,
				Capacity:     make(apiv1.ResourceList),
				Allocatable:  make(apiv1.ResourceList),
			}
			instanceResources[instanceType] = resources
		}

		for resourceName, capacity := range node.Status.Capacity {
			updateMinResource(resources.Capacity, resourceName, capacity)
		}
		for resourceName, allocatable := range node.Status.Allocatable {
			updateMinResource(resources.Allocatable, resourceName, allocatable)
			if capacity, ok := resources.Capacity[resourceName]; ok {
				reserved := sub(capacity, allocatable)
				updateMaxResource(globalReserved, resourceName, reserved)
			}
		}
	}

	// Overwrite the cached data with up-to-date values
	for instanceType, resources := range instanceResources {
		m.instanceResourceCache[instanceType] = resources
	}
	m.reservedResources = globalReserved
}

func updateMinResource(resourceList apiv1.ResourceList, resourceName apiv1.ResourceName, quantity resource.Quantity) {
	current, ok := resourceList[resourceName]
	if !ok || quantity.Cmp(current) < 0 {
		resourceList[resourceName] = quantity
	}
}

func updateMaxResource(resourceList apiv1.ResourceList, resourceName apiv1.ResourceName, quantity resource.Quantity) {
	current, ok := resourceList[resourceName]
	if !ok || quantity.Cmp(current) > 0 {
		resourceList[resourceName] = quantity
	}
}

func sub(q1, q2 resource.Quantity) resource.Quantity {
	result := q1.DeepCopy()
	result.Sub(q2)
	return result
}

func buildGenericLabels(template *asgTemplate, nodeName string) map[string]string {
	result := make(map[string]string)
	// TODO: extract it somehow
	result[kubeletapis.LabelArch] = cloudprovider.DefaultArch
	result[kubeletapis.LabelOS] = cloudprovider.DefaultOS

	if len(template.AvailableResources) == 1 {
		result[kubeletapis.LabelInstanceType] = template.AvailableResources[0].InstanceType
	} else {
		result[kubeletapis.LabelInstanceType] = "<multiple>"
	}

	result[kubeletapis.LabelZoneRegion] = template.Region
	result[kubeletapis.LabelZoneFailureDomain] = template.Zone
	result[kubeletapis.LabelHostname] = nodeName
	return result
}

func extractLabelsFromAsg(tags []*autoscaling.TagDescription) map[string]string {
	result := make(map[string]string)

	for _, tag := range tags {
		k := *tag.Key
		v := *tag.Value
		splits := strings.Split(k, "k8s.io/cluster-autoscaler/node-template/label/")
		if len(splits) > 1 {
			label := splits[1]
			if label != "" {
				result[label] = v
			}
		}
	}

	return result
}

func extractAllocatableResourcesFromAsg(tags []*autoscaling.TagDescription) map[string]*resource.Quantity {
	result := make(map[string]*resource.Quantity)

	for _, tag := range tags {
		k := *tag.Key
		v := *tag.Value
		splits := strings.Split(k, "k8s.io/cluster-autoscaler/node-template/resources/")
		if len(splits) > 1 {
			label := splits[1]
			if label != "" {
				quantity, err := resource.ParseQuantity(v)
				if err != nil {
					continue
				}
				result[label] = &quantity
			}
		}
	}

	return result
}

func extractTaintsFromAsg(tags []*autoscaling.TagDescription) []apiv1.Taint {
	taints := make([]apiv1.Taint, 0)

	for _, tag := range tags {
		k := *tag.Key
		v := *tag.Value
		// The tag value must be in the format <tag>:NoSchedule
		r, _ := regexp.Compile("(.*):(?:NoSchedule|NoExecute|PreferNoSchedule)")
		if r.MatchString(v) {
			splits := strings.Split(k, "k8s.io/cluster-autoscaler/node-template/taint/")
			if len(splits) > 1 {
				values := strings.SplitN(v, ":", 2)
				if len(values) > 1 {
					taints = append(taints, apiv1.Taint{
						Key:    splits[1],
						Value:  values[0],
						Effect: apiv1.TaintEffect(values[1]),
					})
				}
			}
		}
	}
	return taints
}
