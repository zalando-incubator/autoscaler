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
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/autoscaling"
	"github.com/aws/aws-sdk-go/service/ec2"
	"gopkg.in/gcfg.v1"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/utils/gpu"
	"k8s.io/klog"
	provider_aws "k8s.io/kubernetes/pkg/cloudprovider/providers/aws"
	kubeletapis "k8s.io/kubernetes/pkg/kubelet/apis"
)

const (
	operationWaitTimeout    = 5 * time.Second
	operationPollInterval   = 100 * time.Millisecond
	maxRecordsReturnedByAPI = 100
	maxAsgNamesPerDescribe  = 50
	refreshInterval         = 1 * time.Minute
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

func validateOverrides(cfg *provider_aws.CloudConfig) error {
	if len(cfg.ServiceOverride) == 0 {
		return nil
	}
	set := make(map[string]bool)
	for onum, ovrd := range cfg.ServiceOverride {
		// Note: gcfg does not space trim, so we have to when comparing to empty string ""
		name := strings.TrimSpace(ovrd.Service)
		if name == "" {
			return fmt.Errorf("service name is missing [Service is \"\"] in override %s", onum)
		}
		// insure the map service name is space trimmed
		ovrd.Service = name

		region := strings.TrimSpace(ovrd.Region)
		if region == "" {
			return fmt.Errorf("service region is missing [Region is \"\"] in override %s", onum)
		}
		// insure the map region is space trimmed
		ovrd.Region = region

		url := strings.TrimSpace(ovrd.URL)
		if url == "" {
			return fmt.Errorf("url is missing [URL is \"\"] in override %s", onum)
		}
		signingRegion := strings.TrimSpace(ovrd.SigningRegion)
		if signingRegion == "" {
			return fmt.Errorf("signingRegion is missing [SigningRegion is \"\"] in override %s", onum)
		}
		signature := name + "_" + region
		if set[signature] {
			return fmt.Errorf("duplicate entry found for service override [%s] (%s in %s)", onum, name, region)
		}
		set[signature] = true
	}
	return nil
}

func getResolver(cfg *provider_aws.CloudConfig) endpoints.ResolverFunc {
	defaultResolver := endpoints.DefaultResolver()
	defaultResolverFn := func(service, region string,
		optFns ...func(*endpoints.Options)) (endpoints.ResolvedEndpoint, error) {
		return defaultResolver.EndpointFor(service, region, optFns...)
	}
	if len(cfg.ServiceOverride) == 0 {
		return defaultResolverFn
	}

	return func(service, region string,
		optFns ...func(*endpoints.Options)) (endpoints.ResolvedEndpoint, error) {
		for _, override := range cfg.ServiceOverride {
			if override.Service == service && override.Region == region {
				return endpoints.ResolvedEndpoint{
					URL:           override.URL,
					SigningRegion: override.SigningRegion,
					SigningMethod: override.SigningMethod,
					SigningName:   override.SigningName,
				}, nil
			}
		}
		return defaultResolver.EndpointFor(service, region, optFns...)
	}
}

type awsSDKProvider struct {
	cfg *provider_aws.CloudConfig
}

func newAWSSDKProvider(cfg *provider_aws.CloudConfig) *awsSDKProvider {
	return &awsSDKProvider{
		cfg: cfg,
	}
}

// getRegion deduces the current AWS Region.
func getRegion(cfg ...*aws.Config) string {
	region, present := os.LookupEnv("AWS_REGION")
	if !present {
		svc := ec2metadata.New(session.New(), cfg...)
		if r, err := svc.Region(); err == nil {
			region = r
		}
	}
	return region
}

// createAwsManagerInternal allows for a customer autoScalingWrapper to be passed in by tests
//
// #1449 If running tests outside of AWS without AWS_REGION among environment
// variables, avoid a 5+ second EC2 Metadata lookup timeout in getRegion by
// setting and resetting AWS_REGION before calling createAWSManagerInternal:
//
//	defer resetAWSRegion(os.LookupEnv("AWS_REGION"))
//	os.Setenv("AWS_REGION", "fanghorn")
func createAWSManagerInternal(
	configReader io.Reader,
	discoveryOpts cloudprovider.NodeGroupDiscoveryOptions,
	autoScalingService *autoScalingWrapper,
	ec2Service *ec2Wrapper,
) (*AwsManager, error) {

	cfg, err := readAWSCloudConfig(configReader)
	if err != nil {
		klog.Errorf("Couldn't read config: %v", err)
		return nil, err
	}

	if err = validateOverrides(cfg); err != nil {
		klog.Errorf("Unable to validate custom endpoint overrides: %v", err)
		return nil, err
	}

	if autoScalingService == nil || ec2Service == nil {
		awsSdkProvider := newAWSSDKProvider(cfg)
		sess := session.New(aws.NewConfig().WithRegion(getRegion()).
			WithEndpointResolver(getResolver(awsSdkProvider.cfg)))

		if autoScalingService == nil {
			autoScalingService = &autoScalingWrapper{autoscaling.New(sess), map[string]string{}}
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

// readAWSCloudConfig reads an instance of AWSCloudConfig from config reader.
func readAWSCloudConfig(config io.Reader) (*provider_aws.CloudConfig, error) {
	var cfg provider_aws.CloudConfig
	var err error

	if config != nil {
		err = gcfg.ReadInto(&cfg, config)
		if err != nil {
			return nil, err
		}
	}

	return &cfg, nil
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
		klog.Errorf("Failed to regenerate ASG cache: %v", err)
		return err
	}
	m.lastRefresh = time.Now()
	klog.V(2).Infof("Refreshed ASG list, next refresh after %v", m.lastRefresh.Add(refreshInterval))
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
	if err := m.asgCache.DeleteInstances(instances); err != nil {
		return err
	}
	klog.V(2).Infof("Some ASG instances might have been deleted, forcing ASG list refresh")
	return m.forceRefresh()
}

// GetAsgNodes returns Asg nodes.
func (m *AwsManager) GetAsgNodes(ref AwsRef) ([]AwsInstanceRef, error) {
	return m.asgCache.InstancesByAsg(ref)
}

func (m *AwsManager) getAsgTemplate(asg *asg) (*asgTemplate, error) {
	if len(asg.AvailabilityZones) < 1 {
		return nil, fmt.Errorf("unable to get first AvailabilityZone for ASG %q", asg.Name)
	}

	az := asg.AvailabilityZones[0]
	region := az[0 : len(az)-1]

	if len(asg.AvailabilityZones) > 1 {
		klog.Warningf("Found multiple availability zones for ASG %q; using %s\n", asg.Name, az)
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
		instanceType, ok := node.GetLabels()[apiv1.LabelInstanceType]
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
		result[apiv1.LabelInstanceType] = template.AvailableResources[0].InstanceType
	} else {
		result[apiv1.LabelInstanceType] = "<multiple>"
	}

	result[apiv1.LabelZoneRegion] = template.Region
	result[apiv1.LabelZoneFailureDomain] = template.Zone
	result[apiv1.LabelHostname] = nodeName
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
