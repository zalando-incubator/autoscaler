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

package core

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	rand "math/rand"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1beta1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	kuberuntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/clusterstate"
	"k8s.io/autoscaler/cluster-autoscaler/clusterstate/api"
	"k8s.io/autoscaler/cluster-autoscaler/clusterstate/utils"
	"k8s.io/autoscaler/cluster-autoscaler/config"
	scalercontext "k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/estimator"
	"k8s.io/autoscaler/cluster-autoscaler/expander"
	"k8s.io/autoscaler/cluster-autoscaler/expander/highestpriority"
	"k8s.io/autoscaler/cluster-autoscaler/simulator"
	"k8s.io/autoscaler/cluster-autoscaler/utils/backoff"
	"k8s.io/autoscaler/cluster-autoscaler/utils/deletetaint"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	kube_util "k8s.io/autoscaler/cluster-autoscaler/utils/kubernetes"
	"k8s.io/autoscaler/cluster-autoscaler/utils/scheduler"
	"k8s.io/autoscaler/cluster-autoscaler/utils/units"
	"k8s.io/client-go/kubernetes/fake"
	v1appslister "k8s.io/client-go/listers/apps/v1"
	v1batchlister "k8s.io/client-go/listers/batch/v1"
	v1corelister "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/listers/policy/v1beta1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog"
	schedulernodeinfo "k8s.io/kubernetes/pkg/scheduler/nodeinfo"
)

type zalandoCloudProviderCommandType string

const (
	zalandoCloudProviderCommandIncreaseSize       zalandoCloudProviderCommandType = "increaseSize"
	zalandoCloudProviderCommandDeleteNodes        zalandoCloudProviderCommandType = "deleteNodes"
	zalandoCloudProviderCommandDecreaseTargetSize zalandoCloudProviderCommandType = "decreaseTargetSize"

	testNamespace = "test"
)

type customTimeLogWriter struct {
	lock    sync.Mutex
	testEnv *zalandoTestEnv
}

func (w *customTimeLogWriter) Write(p []byte) (int, error) {
	w.lock.Lock()
	defer w.lock.Unlock()

	if w.testEnv == nil {
		return os.Stderr.Write(p)
	}

	w.testEnv.currentTimeLock.Lock()
	defer w.testEnv.currentTimeLock.Unlock()

	s := fmt.Sprintf("[%8s] %s", w.testEnv.currentTime.Sub(w.testEnv.initialTime), string(p))
	_, err := os.Stderr.Write([]byte(s))
	if err != nil {
		return 0, err
	}
	return len(p), nil
}

func (w *customTimeLogWriter) setTestEnvironment(env *zalandoTestEnv) {
	w.lock.Lock()
	defer w.lock.Unlock()

	if w.testEnv != nil && env != nil {
		panic("test environment already set")
	}

	w.testEnv = env
}

var (
	logWriter = &customTimeLogWriter{}
)

func redirectLogging() {
	klog.SetOutput(logWriter)
	err := flag.Set("logtostderr", "false")
	if err != nil {
		panic(err)
	}
}

type zalandoCloudProviderCommand struct {
	commandType zalandoCloudProviderCommandType
	nodeGroup   string
	delta       int      // for increase/decrease size commands
	nodeNames   []string // for deleteNodes
}

func (cmd zalandoCloudProviderCommand) String() string {
	switch cmd.commandType {
	case zalandoCloudProviderCommandIncreaseSize:
		return fmt.Sprintf("increaseSize(%s, %+d)", cmd.nodeGroup, cmd.delta)
	case zalandoCloudProviderCommandDeleteNodes:
		return fmt.Sprintf("deleteNodes(%s, %s)", cmd.nodeGroup, strings.Join(cmd.nodeNames, ", "))
	case zalandoCloudProviderCommandDecreaseTargetSize:
		return fmt.Sprintf("decreaseTargetSize(%s, %+d)", cmd.nodeGroup, cmd.delta)
	default:
		return fmt.Sprintf("<invalid: %s>", cmd.commandType)
	}
}

type zalandoTestCloudProvider struct {
	limiter            *cloudprovider.ResourceLimiter
	expectedGID        uint64
	nodeGroups         []*zalandoTestCloudProviderNodeGroup
	instanceCacheValid bool
}

func newZalandoTestCloudProvider(limiter *cloudprovider.ResourceLimiter, expectedGID uint64) *zalandoTestCloudProvider {
	return &zalandoTestCloudProvider{
		limiter:     limiter,
		expectedGID: expectedGID,
	}
}

func (p *zalandoTestCloudProvider) Name() string {
	return "zalando-test"
}

func (p *zalandoTestCloudProvider) NodeGroups() []cloudprovider.NodeGroup {
	ensureSameGoroutine(p.expectedGID)

	var result []cloudprovider.NodeGroup
	for _, group := range p.nodeGroups {
		result = append(result, group)
	}
	return result
}

func (p *zalandoTestCloudProvider) nodeGroup(id string) (*zalandoTestCloudProviderNodeGroup, error) {
	ensureSameGoroutine(p.expectedGID)

	for _, group := range p.nodeGroups {
		if group.id == id {
			return group, nil
		}
	}
	return nil, fmt.Errorf("unable to find node group %s", id)
}

func (p *zalandoTestCloudProvider) NodeGroupForNode(node *corev1.Node) (cloudprovider.NodeGroup, error) {
	ensureSameGoroutine(p.expectedGID)
	providerId := node.Spec.ProviderID
	groupId := strings.Split(strings.TrimPrefix(providerId, "zalando-test:///"), "/")[0]

	for _, group := range p.nodeGroups {
		if group.id == groupId {
			return group, nil
		}
	}

	return nil, fmt.Errorf("unable to find node group for node %s (%s)", node.Name, providerId)
}

func (p *zalandoTestCloudProvider) Pricing() (cloudprovider.PricingModel, errors.AutoscalerError) {
	return nil, cloudprovider.ErrNotImplemented
}

func (p *zalandoTestCloudProvider) GetAvailableMachineTypes() ([]string, error) {
	return []string{}, nil
}

func (p *zalandoTestCloudProvider) NewNodeGroup(machineType string, labels map[string]string, systemLabels map[string]string, taints []corev1.Taint, extraResources map[string]resource.Quantity) (cloudprovider.NodeGroup, error) {
	return nil, cloudprovider.ErrNotImplemented
}

func (p *zalandoTestCloudProvider) GetResourceLimiter() (*cloudprovider.ResourceLimiter, error) {
	return p.limiter, nil
}

func (p *zalandoTestCloudProvider) GPULabel() string {
	return "k8s.amazonaws.com/accelerator"
}

func (p *zalandoTestCloudProvider) GetAvailableGPUTypes() map[string]struct{} {
	return map[string]struct{}{
		"nvidia-tesla-k80":  {},
		"nvidia-tesla-p100": {},
		"nvidia-tesla-v100": {},
	}
}

func (p *zalandoTestCloudProvider) Cleanup() error {
	return nil
}

func (p *zalandoTestCloudProvider) Refresh(existingNodes []*corev1.Node) error {
	if !p.instanceCacheValid {
		klog.V(3).Infof("Regenerating cached instances...")
		for _, group := range p.nodeGroups {
			group.regenerateCachedInstances()
		}
		p.instanceCacheValid = true
	}

	return nil
}

type zalandoTestCloudProviderNodeGroup struct {
	id            string
	expectedGID   uint64
	maxSize       int
	targetSize    int
	instances     sets.String
	templateNode  *schedulernodeinfo.NodeInfo
	handleCommand func(command zalandoCloudProviderCommand)

	cachedInstances []cloudprovider.Instance
}

func (g *zalandoTestCloudProviderNodeGroup) MaxSize() int {
	return g.maxSize
}

func (g *zalandoTestCloudProviderNodeGroup) MinSize() int {
	return 0
}

func (g *zalandoTestCloudProviderNodeGroup) TargetSize() (int, error) {
	ensureSameGoroutine(g.expectedGID)

	return g.targetSize, nil
}

func (g *zalandoTestCloudProviderNodeGroup) IncreaseSize(delta int) error {
	g.handleCommand(zalandoCloudProviderCommand{
		commandType: zalandoCloudProviderCommandIncreaseSize,
		nodeGroup:   g.id,
		delta:       delta,
	})
	return nil
}

func (g *zalandoTestCloudProviderNodeGroup) DeleteNodes(nodes []*corev1.Node) error {
	var nodeNames []string
	for _, node := range nodes {
		nodeNames = append(nodeNames, node.Name)
	}
	sort.Strings(nodeNames)
	g.handleCommand(zalandoCloudProviderCommand{
		commandType: zalandoCloudProviderCommandDeleteNodes,
		nodeGroup:   g.id,
		nodeNames:   nodeNames,
	})
	g.regenerateCachedInstances()
	return nil
}

func (g *zalandoTestCloudProviderNodeGroup) DecreaseTargetSize(delta int) error {
	g.handleCommand(zalandoCloudProviderCommand{
		commandType: zalandoCloudProviderCommandDecreaseTargetSize,
		nodeGroup:   g.id,
		delta:       delta,
	})
	return nil
}

func (g *zalandoTestCloudProviderNodeGroup) Id() string {
	return g.id
}

func (g *zalandoTestCloudProviderNodeGroup) Debug() string {
	return fmt.Sprintf("%s (%d instances, %d desired, %d max)", g.id, len(g.instances), g.targetSize, g.maxSize)
}

func (g *zalandoTestCloudProviderNodeGroup) Nodes() ([]cloudprovider.Instance, error) {
	ensureSameGoroutine(g.expectedGID)

	return g.cachedInstances, nil
}

func (g *zalandoTestCloudProviderNodeGroup) regenerateCachedInstances() {
	var result []cloudprovider.Instance
	for instance := range g.instances {
		result = append(result, cloudprovider.Instance{
			Id: fmt.Sprintf("zalando-test:///%s/%s", g.id, instance),
		})
	}
	for i := 0; i < g.targetSize-len(g.instances); i++ {
		result = append(result, cloudprovider.Instance{
			Id: fmt.Sprintf("zalando-test:///%s/i-placeholder-%d", g.id, i),
		})
	}
	g.cachedInstances = result
}

func (g *zalandoTestCloudProviderNodeGroup) TemplateNodeInfo() (*schedulernodeinfo.NodeInfo, error) {
	ensureSameGoroutine(g.expectedGID)

	result := scheduler.CloneNodeInfo(g.templateNode)
	result.Node().Name = fmt.Sprintf("%s-asg-%d", g.id, rand.Int63())
	return result, nil
}

func (g *zalandoTestCloudProviderNodeGroup) Exist() bool {
	return true
}

func (g *zalandoTestCloudProviderNodeGroup) Create() (cloudprovider.NodeGroup, error) {
	return nil, cloudprovider.ErrNotImplemented
}

func (g *zalandoTestCloudProviderNodeGroup) Delete() error {
	return cloudprovider.ErrNotImplemented
}

func (g *zalandoTestCloudProviderNodeGroup) Autoprovisioned() bool {
	return false
}

func (g *zalandoTestCloudProviderNodeGroup) setTemplateNode(cpu resource.Quantity, memory resource.Quantity, nodeLabels map[string]string) error {
	templateNode := schedulernodeinfo.NewNodeInfo()
	err := templateNode.SetNode(&corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Labels: nodeLabels,
		},
		Status: corev1.NodeStatus{
			Capacity: corev1.ResourceList{
				corev1.ResourceCPU:    cpu,
				corev1.ResourceMemory: memory,
				corev1.ResourcePods:   resource.MustParse("110"),
			},
			Allocatable: corev1.ResourceList{
				corev1.ResourceCPU:    cpu,
				corev1.ResourceMemory: memory,
				corev1.ResourcePods:   resource.MustParse("110"),
			},
		},
	})
	if err != nil {
		return err
	}

	g.templateNode = templateNode
	return nil
}

func defaultZalandoAutoscalingOptions() config.AutoscalingOptions {
	return config.AutoscalingOptions{
		// defaults
		CloudConfig:                      "",
		CloudProviderName:                "",
		NodeGroupAutoDiscovery:           nil,
		MaxTotalUnreadyPercentage:        45,
		OkTotalUnreadyCount:              3,
		ScaleUpFromZero:                  true,
		EstimatorName:                    estimator.BinpackingEstimatorName,
		IgnoreDaemonSetsUtilization:      false,
		IgnoreMirrorPodsUtilization:      false,
		MaxBulkSoftTaintCount:            10,
		MaxBulkSoftTaintTime:             3 * time.Second,
		MaxEmptyBulkDelete:               10,
		MaxGracefulTerminationSec:        10 * 60,
		MaxCoresTotal:                    config.DefaultMaxClusterCores,
		MinCoresTotal:                    0,
		MaxMemoryTotal:                   config.DefaultMaxClusterMemory * units.GiB,
		MinMemoryTotal:                   0,
		GpuTotal:                         nil,
		NodeGroups:                       nil,
		ScaleDownDelayAfterDelete:        0,
		ScaleDownDelayAfterFailure:       3 * time.Minute,
		ScaleDownUnreadyTime:             20 * time.Minute,
		ScaleDownGpuUtilizationThreshold: 0.5,
		ScaleDownNonEmptyCandidatesCount: 30,
		ScaleDownCandidatesPoolRatio:     0.1,
		ScaleDownCandidatesPoolMinCount:  50,
		WriteStatusConfigMap:             true,
		ConfigNamespace:                  "kube-system",
		ClusterName:                      "",
		NodeAutoprovisioningEnabled:      false,
		MaxAutoprovisionedNodeGroupCount: 15,
		UnremovableNodeRecheckTimeout:    5 * time.Minute,
		Regional:                         false,
		NewPodScaleUpDelay:               0 * time.Second,
		IgnoredTaints:                    nil,
		KubeConfigPath:                   "",
		NodeDeletionDelayTimeout:         2 * time.Minute,
		AWSUseStaticInstanceList:         false,
		MaxPodEvictionTime:               2 * time.Minute,

		// customized
		ExpanderName:                        expander.HighestPriorityExpanderName,
		ExpendablePodsPriorityCutoff:        -1000000,
		ScaleDownEnabled:                    true,
		ScaleDownDelayAfterAdd:              -1 * time.Second,
		ScaleDownUnneededTime:               10 * time.Minute,
		ScaleDownUtilizationThreshold:       1.0,
		BalanceSimilarNodeGroups:            true,
		MaxNodeProvisionTime:                7 * time.Minute,
		MaxNodesTotal:                       10000,
		ScaleUpTemplateFromCloudProvider:    true,
		BackoffNoFullScaleDown:              true,
		TopologySpreadConstraintSplitFactor: 3,
		DisableNodeInstancesCache:           true,
	}
}

type zalandoTestEnv struct {
	// Only used when reading from other threads and writing
	currentTimeLock sync.Mutex
	currentTime     time.Time

	t                            *testing.T
	expectedGID                  uint64
	interval                     time.Duration
	client                       *fake.Clientset
	initialTime                  time.Time
	instanceCacheResetTime       time.Time
	pdbIndexer                   cache.Indexer
	daemonsetIndexer             cache.Indexer
	replicationControllerIndexer cache.Indexer
	jobIndexer                   cache.Indexer
	replicasetIndexer            cache.Indexer
	statefulsetIndexer           cache.Indexer
	cloudProvider                *zalandoTestCloudProvider
	autoscaler                   *StaticAutoscaler
	pendingCommands              []zalandoCloudProviderCommand
}

func (e *zalandoTestEnv) AddNodeGroup(id string, maxSize int, nodeCPU, nodeMemory resource.Quantity, nodeLabels map[string]string) *zalandoTestEnv {
	for _, group := range e.cloudProvider.nodeGroups {
		if group.id == id {
			e.t.Errorf("node group already exists: %s", id)
		}
	}

	ng := &zalandoTestCloudProviderNodeGroup{
		id:            id,
		expectedGID:   e.expectedGID,
		maxSize:       maxSize,
		instances:     sets.NewString(),
		handleCommand: e.handleCommand,
	}
	err := ng.setTemplateNode(nodeCPU, nodeMemory, nodeLabels)
	require.NoError(e.t, err)
	e.cloudProvider.nodeGroups = append(e.cloudProvider.nodeGroups, ng)
	klog.Infof("Added node group %s with max size %d, %s cpu, %s memory and labels %s", id, maxSize, &nodeCPU, &nodeMemory, nodeLabels)
	return e
}

func (e *zalandoTestEnv) replicaSetsUpdated() {
	var result []interface{}
	res, err := e.client.AppsV1().ReplicaSets(corev1.NamespaceAll).List(context.Background(), metav1.ListOptions{})
	require.NoError(e.t, err)
	for _, item := range res.Items {
		item := item
		result = append(result, item)
	}
}

func (e *zalandoTestEnv) AddReplicaSet(replicaset *appsv1.ReplicaSet) *zalandoTestEnv {
	_, err := e.client.AppsV1().ReplicaSets(replicaset.Namespace).Create(context.Background(), replicaset, metav1.CreateOptions{})
	require.NoError(e.t, err)
	e.replicaSetsUpdated()
	klog.Infof("Added ReplicaSet %s/%s", replicaset.Namespace, replicaset.Name)
	return e
}

func (e *zalandoTestEnv) AddPod(pod *corev1.Pod) *zalandoTestEnv {
	_, err := e.client.CoreV1().Pods(pod.Namespace).Create(context.Background(), pod, metav1.CreateOptions{})
	require.NoError(e.t, err)
	klog.Infof("Added Pod %s/%s", pod.Namespace, pod.Name)
	return e
}

func (e *zalandoTestEnv) nodesPendingDeletion() sets.String {
	result := sets.NewString()
	nodes, err := e.client.CoreV1().Nodes().List(context.Background(), metav1.ListOptions{})
	require.NoError(e.t, err)
	for _, node := range nodes.Items {
		if deletetaint.HasToBeDeletedTaint(&node) {
			result.Insert(node.Name)
		}
	}
	return result
}

func (e *zalandoTestEnv) StepOnce() *zalandoTestEnv {
	// Emulate the AWS cloud provider behaviour
	if e.instanceCacheResetTime.Add(time.Minute).Before(e.currentTime) {
		e.cloudProvider.instanceCacheValid = false
		e.instanceCacheResetTime = e.currentTime
	}

	err := e.autoscaler.RunOnce(e.currentTime)
	require.NoError(e.t, err)

	e.currentTimeLock.Lock()
	defer e.currentTimeLock.Unlock()
	e.currentTime = e.currentTime.Add(e.interval)
	return e
}

func (e *zalandoTestEnv) StepFor(duration time.Duration) *zalandoTestEnv {
	deadline := e.currentTime.Add(duration)
	for {
		if !e.currentTime.Before(deadline) {
			return e
		}
		e.StepOnce()
	}
}

func (e *zalandoTestEnv) StepUntilNextCommand(timeout time.Duration) *zalandoTestEnv {
	deadline := e.currentTime.Add(timeout)
	for {
		if len(e.pendingCommands) > 0 {
			return e
		}
		if !e.currentTime.Before(deadline) {
			require.FailNow(e.t, "StepUntilNextCommand timeout")
			return e
		}
		e.StepOnce()
	}
}

func (e *zalandoTestEnv) StepUntilCommand(maxTime time.Duration, command zalandoCloudProviderCommand) *zalandoTestEnv {
	deadline := e.currentTime.Add(maxTime)
	for {
		for _, cmd := range e.pendingCommands {
			if assert.ObjectsAreEqualValues(command, cmd) {
				return e
			}
		}
		if !e.currentTime.Before(deadline) {
			require.FailNow(e.t, "StepUntilCommand timeout")
			return e
		}
		e.StepOnce()
	}
}

func (e *zalandoTestEnv) ConsumeCommands() []zalandoCloudProviderCommand {
	result := e.pendingCommands
	e.pendingCommands = nil
	return result
}

func (e *zalandoTestEnv) ExpectNoCommands() *zalandoTestEnv {
	require.Empty(e.t, e.pendingCommands)
	return e
}

func (e *zalandoTestEnv) ExpectCommands(commands ...zalandoCloudProviderCommand) *zalandoTestEnv {
	require.EqualValues(e.t, commands, e.pendingCommands)
	e.pendingCommands = nil
	return e
}

func (e *zalandoTestEnv) handleCommand(command zalandoCloudProviderCommand) {
	ensureSameGoroutine(e.expectedGID)

	klog.Infof("Received a node group command: %s", command)

	switch command.commandType {
	case zalandoCloudProviderCommandIncreaseSize:
		ng, err := e.cloudProvider.nodeGroup(command.nodeGroup)
		require.NoError(e.t, err)
		require.True(e.t, command.delta >= 0, "attempted to increase size of %s with invalid delta: %d", command.nodeGroup, command.delta)
		require.True(e.t, ng.targetSize+command.delta <= ng.maxSize, "attempted to increase size of %s beyond max: current %d, delta %d", command.nodeGroup, ng.targetSize, command.delta)

		ng.targetSize += command.delta
	case zalandoCloudProviderCommandDecreaseTargetSize:
		ng, err := e.cloudProvider.nodeGroup(command.nodeGroup)
		require.NoError(e.t, err)
		require.True(e.t, command.delta <= 0, "attempted to decrease size of %s with invalid delta: %d", command.nodeGroup, command.delta)
		require.True(e.t, ng.targetSize+command.delta >= len(ng.instances), "attempted to decrease size of %s beyond the number of instances: current %d, delta %d, instances %d", command.nodeGroup, ng.targetSize, command.delta, len(ng.instances))

		ng.targetSize += command.delta
	case zalandoCloudProviderCommandDeleteNodes:
		ng, err := e.cloudProvider.nodeGroup(command.nodeGroup)
		require.NoError(e.t, err)

		for _, name := range command.nodeNames {
			if !strings.Contains(name, "-placeholder-") {
				require.True(e.t, ng.instances.Has(name), "instance not found in %s: %s", command.nodeGroup, name)
			}
		}
		ng.targetSize -= len(command.nodeNames)
	default:
		require.FailNowf(e.t, "invalid command", "received invalid command: %s", command.commandType)
	}

	e.pendingCommands = append(e.pendingCommands, command)
}

func (e *zalandoTestEnv) AddInstance(nodeGroup string, instanceId string, incrementTargetSize bool) *zalandoTestEnv {
	for _, group := range e.cloudProvider.nodeGroups {
		require.False(e.t, group.instances.Has(instanceId), "instance already exists: %s", instanceId)
	}

	ng, err := e.cloudProvider.nodeGroup(nodeGroup)
	require.NoError(e.t, err)

	currentTargetSize := ng.targetSize

	ng.instances.Insert(instanceId)
	if incrementTargetSize {
		ng.targetSize++
	}
	klog.Infof("Added instance %s for node group %s (target size %d -> %d)", instanceId, nodeGroup, currentTargetSize, ng.targetSize)
	return e
}

func (e *zalandoTestEnv) AddNode(instanceId string, ready bool) *zalandoTestEnv {
	for _, group := range e.cloudProvider.nodeGroups {
		if group.instances.Has(instanceId) {
			node := group.templateNode.Node().DeepCopy()
			node.CreationTimestamp = metav1.Time{Time: e.currentTime}
			node.Name = instanceId
			node.Spec.ProviderID = fmt.Sprintf("zalando-test:///%s/%s", group.id, instanceId)

			if ready {
				node.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}
			} else {
				node.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionFalse}}
				node.Spec.Taints = []corev1.Taint{{Key: "node.kubernetes.io/not-ready", Effect: corev1.TaintEffectNoSchedule}}
			}

			_, err := e.client.CoreV1().Nodes().Create(context.Background(), node, metav1.CreateOptions{})
			require.NoError(e.t, err)

			readiness := "a ready"
			if !ready {
				readiness = "an unready"
			}

			klog.Infof("Added %s node for instance %s in node group %s", readiness, instanceId, group.id)
			return e
		}
	}

	require.FailNowf(e.t, "invalid instance", "instance %s doesn't belong to any node groups", instanceId)
	return e
}

func (e *zalandoTestEnv) AddScheduledPod(pod *corev1.Pod, nodeName string) *zalandoTestEnv {
	return e.AddPod(pod).SchedulePod(pod, nodeName)
}

func (e *zalandoTestEnv) SchedulePod(pod *corev1.Pod, nodeName string) *zalandoTestEnv {
	_, err := e.client.CoreV1().Nodes().Get(context.Background(), nodeName, metav1.GetOptions{})
	require.NoError(e.t, err, "unknown node: %s", nodeName)

	pod, err = e.client.CoreV1().Pods(pod.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
	require.NoError(e.t, err)

	require.Empty(e.t, pod.Spec.NodeName, "pod already scheduled on %s", pod.Spec.NodeName)
	pod.Spec.NodeName = nodeName
	_, err = e.client.CoreV1().Pods(pod.Namespace).Update(context.Background(), pod, metav1.UpdateOptions{})
	require.NoError(e.t, err)

	klog.Infof("Scheduled pod %s/%s on node %s", pod.Namespace, pod.Name, nodeName)
	return e
}

func (e *zalandoTestEnv) RemovePod(pod *corev1.Pod) *zalandoTestEnv {
	current, err := e.client.CoreV1().Pods(pod.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
	require.NoError(e.t, err)

	err = e.client.CoreV1().Pods(pod.Namespace).Delete(context.Background(), pod.Name, metav1.DeleteOptions{})
	require.NoError(e.t, err)

	if current.Spec.NodeName == "" {
		klog.Infof("Removed pod %s/%s (unscheduled)", current.Namespace, current.Name)
	} else {
		klog.Infof("Removed pod %s/%s (running on %s)", current.Namespace, current.Name, current.Spec.NodeName)
	}
	return e
}

func (e *zalandoTestEnv) RemoveNode(name string, decrementTargetSize bool) {
	node, err := e.client.CoreV1().Nodes().Get(context.Background(), name, metav1.GetOptions{})
	require.NoError(e.t, err)

	ng, err := e.cloudProvider.NodeGroupForNode(node)
	require.NoError(e.t, err)

	zalandoNodeGroup := ng.(*zalandoTestCloudProviderNodeGroup)
	require.True(e.t, zalandoNodeGroup.instances.Has(name), "instance not found in node group: %s", name)

	// Delete the node
	err = e.client.CoreV1().Nodes().Delete(context.Background(), name, metav1.DeleteOptions{})
	require.NoError(e.t, err)

	// Delete pods scheduled on it
	pods, err := e.client.CoreV1().Pods(corev1.NamespaceAll).List(context.Background(), metav1.ListOptions{})
	require.NoError(e.t, err)
	for _, item := range pods.Items {
		if item.Spec.NodeName == name {
			e.RemovePod(&item)
		}
	}

	currentTargetSize := zalandoNodeGroup.targetSize

	// Delete the instance and decrease the target size
	zalandoNodeGroup.instances.Delete(name)
	if decrementTargetSize {
		zalandoNodeGroup.targetSize--
	}

	klog.Infof("Removed instance %s for node group %s (target size %d -> %d)", name, zalandoNodeGroup.id, currentTargetSize, zalandoNodeGroup.targetSize)
}

func (e *zalandoTestEnv) CurrentTime() time.Duration {
	return e.currentTime.Sub(e.initialTime)
}

func (e *zalandoTestEnv) GetClusterStatus() *api.ClusterAutoscalerStatus {
	return e.autoscaler.clusterStateRegistry.GetStatus(e.currentTime)
}

func (e *zalandoTestEnv) GetScaleUpFailures() map[string][]clusterstate.ScaleUpFailure {
	return e.autoscaler.clusterStateRegistry.GetScaleUpFailures()
}

func formatCondition(condition api.ClusterAutoscalerCondition) string {
	return fmt.Sprintf("type=%s status=%s reason=%s lastTransition=%s message=%s", condition.Type, condition.Status, condition.Reason, condition.LastTransitionTime, condition.Message)
}

func (e *zalandoTestEnv) LogStatus() *zalandoTestEnv {
	status := e.autoscaler.clusterStateRegistry.GetStatus(e.currentTime)
	for _, groupStatus := range status.NodeGroupStatuses {
		klog.Infof("Node group %s:", groupStatus.ProviderID)
		for _, condition := range groupStatus.Conditions {
			klog.Infof("  - %s", formatCondition(condition))
		}
	}

	klog.Info("Clusterwide:")
	for _, condition := range status.ClusterwideConditions {
		klog.Infof("  - %s", formatCondition(condition))
	}
	return e
}

func (e *zalandoTestEnv) ExpectTargetSize(nodeGroup string, size int) *zalandoTestEnv {
	ng, err := e.cloudProvider.nodeGroup(nodeGroup)
	require.NoError(e.t, err)
	require.Equal(e.t, size, ng.targetSize)
	return e
}

func (e *zalandoTestEnv) nodeGroupConditionStatus(nodeGroup string, conditionType api.ClusterAutoscalerConditionType) (*api.ClusterAutoscalerCondition, error) {
	for _, status := range e.autoscaler.clusterStateRegistry.GetStatus(e.currentTime).NodeGroupStatuses {
		if status.ProviderID == nodeGroup {
			for _, condition := range status.Conditions {
				if condition.Type == conditionType {
					return &condition, nil
				}
			}
			return nil, fmt.Errorf("no %s condition found for node group %s", conditionType, nodeGroup)
		}
	}
	return nil, fmt.Errorf("no status for node group %s", nodeGroup)
}

func (e *zalandoTestEnv) ExpectBackedOff(nodeGroup string) *zalandoTestEnv {
	cond, err := e.nodeGroupConditionStatus(nodeGroup, api.ClusterAutoscalerScaleUp)
	require.NoError(e.t, err)
	require.Equal(e.t, api.ClusterAutoscalerBackoff, cond.Status)
	return e
}

func (e *zalandoTestEnv) ExpectNotBackedOff(nodeGroup string) *zalandoTestEnv {
	cond, err := e.nodeGroupConditionStatus(nodeGroup, api.ClusterAutoscalerScaleUp)
	require.NoError(e.t, err)
	require.NotEqual(e.t, api.ClusterAutoscalerBackoff, cond.Status)
	return e
}

func (e *zalandoTestEnv) SetTargetSize(nodeGroup string, targetSize int) *zalandoTestEnv {
	ng, err := e.cloudProvider.nodeGroup(nodeGroup)
	require.NoError(e.t, err)

	require.True(e.t, targetSize >= 0, "target size must be positive: %d", targetSize)
	require.True(e.t, targetSize <= ng.maxSize, "target size must be within ASG limits (%d): %d", ng.maxSize, targetSize)
	require.True(e.t, targetSize >= len(ng.instances), "attempted to decrease size of %s beyond the number of instances: target %d, instances %d", nodeGroup, ng.targetSize, len(ng.instances))
	currentTargetSize := ng.targetSize
	ng.targetSize = targetSize
	klog.Infof("Updated target size for node group %s (%d -> %d)", nodeGroup, currentTargetSize, targetSize)
	return e
}

func (e *zalandoTestEnv) SetMaxSize(nodeGroup string, maxSize int) *zalandoTestEnv {
	ng, err := e.cloudProvider.nodeGroup(nodeGroup)
	require.NoError(e.t, err)

	require.True(e.t, maxSize >= 0, "max size must be positive: %d", maxSize)
	require.True(e.t, maxSize >= len(ng.instances), "max size must not be smaller than the number of instances (%d): %d", len(ng.instances), maxSize)
	require.True(e.t, maxSize >= ng.targetSize, "max size must not be smaller than the target size (%d): %d", ng.targetSize, maxSize)
	currentMaxSize := ng.maxSize
	ng.maxSize = maxSize
	klog.Infof("Updated max size for node group %s (%d -> %d)", nodeGroup, currentMaxSize, maxSize)
	return e
}

func (e *zalandoTestEnv) SetTemplateNode(nodeGroup string, nodeCPU, nodeMemory resource.Quantity, nodeLabels map[string]string) *zalandoTestEnv {
	ng, err := e.cloudProvider.nodeGroup(nodeGroup)
	require.NoError(e.t, err)

	err = ng.setTemplateNode(nodeCPU, nodeMemory, nodeLabels)
	require.NoError(e.t, err)

	klog.Infof("Updated node template for group %s: %s cpu, %s memory and labels %s", nodeGroup, &nodeCPU, &nodeMemory, nodeLabels)
	return e
}

func (e *zalandoTestEnv) RemoveNodeGroup(nodeGroup string) *zalandoTestEnv {
	var updated []*zalandoTestCloudProviderNodeGroup
	found := false

	for _, group := range e.cloudProvider.nodeGroups {
		if group.id == nodeGroup {
			found = true
			continue
		}
		updated = append(updated, group)
	}

	require.True(e.t, found, "Node group %s not found", nodeGroup)

	e.cloudProvider.nodeGroups = updated
	klog.Infof("Removed node group %s", nodeGroup)
	return e
}

type fakeClientNodeLister struct {
	client *fake.Clientset
	filter func(node *corev1.Node) bool
}

func (l *fakeClientNodeLister) List() ([]*corev1.Node, error) {
	res, err := l.client.CoreV1().Nodes().List(context.Background(), metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	var result []*corev1.Node
	for _, node := range res.Items {
		node := node
		if l.filter != nil && !l.filter(&node) {
			continue
		}
		result = append(result, &node)
	}
	return result, nil
}

func (l *fakeClientNodeLister) Get(name string) (*corev1.Node, error) {
	return l.client.CoreV1().Nodes().Get(context.Background(), name, metav1.GetOptions{})
}

type fakeClientPodLister struct {
	client *fake.Clientset
	filter func(pod *corev1.Pod) bool
}

func (l *fakeClientPodLister) List() ([]*corev1.Pod, error) {
	res, err := l.client.CoreV1().Pods(corev1.NamespaceAll).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	var result []*corev1.Pod
	for _, pod := range res.Items {
		pod := pod
		if l.filter != nil && !l.filter(&pod) {
			continue
		}
		result = append(result, &pod)
	}
	return result, nil
}

type pdbLister struct {
	lister v1beta1.PodDisruptionBudgetLister
}

func (l *pdbLister) List() ([]*policyv1.PodDisruptionBudget, error) {
	return l.lister.List(labels.Everything())
}

func makeIndexer() cache.Indexer {
	return cache.NewIndexer(
		func(obj interface{}) (string, error) {
			meta, err := meta.Accessor(obj)
			if err != nil {
				return "", fmt.Errorf("object has no meta: %v", err)
			}
			return fmt.Sprintf("%s/%s", meta.GetNamespace(), meta.GetName()), nil
		},
		map[string]cache.IndexFunc{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
}

type printRecorder struct{}

func (f *printRecorder) Event(object kuberuntime.Object, eventtype, reason, message string) {
	meta, ok := object.(metav1.ObjectMetaAccessor)
	if !ok {
		return
	}
	klog.Infof("Event for %s/%s (%s): %s", meta.GetObjectMeta().GetNamespace(), meta.GetObjectMeta().GetName(), reason, message)
}

func (f *printRecorder) Eventf(object kuberuntime.Object, eventtype, reason, messageFmt string, args ...interface{}) {
	f.Event(object, eventtype, reason, fmt.Sprintf(messageFmt, args...))
}

func (f *printRecorder) AnnotatedEventf(object kuberuntime.Object, annotations map[string]string, eventtype, reason, messageFmt string, args ...interface{}) {
	f.Eventf(object, eventtype, reason, messageFmt, args...)
}

// RunSimulation runs testFn with a freshly created simulation environment
func RunSimulation(t *testing.T, options config.AutoscalingOptions, interval time.Duration, testFn func(env *zalandoTestEnv)) {
	processorCallbacks := newStaticAutoscalerProcessorCallbacks()
	clientset := fake.NewSimpleClientset()

	provider := newZalandoTestCloudProvider(scalercontext.NewResourceLimiterFromAutoscalingOptions(options), getGID())

	recorder := &printRecorder{}
	statusRecorder, err := utils.NewStatusMapRecorder(clientset, "kube-system", recorder, false)
	require.NoError(t, err)

	estimatorBuilder, err := estimator.NewEstimatorBuilder(options.EstimatorName)
	require.NoError(t, err)

	closeChannel := make(chan struct{})
	defer close(closeChannel)
	predicateChecker, err := simulator.NewSchedulerBasedPredicateChecker(clientset, closeChannel)
	require.NoError(t, err)

	pdbIndexer := makeIndexer()
	daemonsetIndexer := makeIndexer()
	replicationControllerIndexer := makeIndexer()
	jobIndexer := makeIndexer()
	replicasetIndexer := makeIndexer()
	statefulsetIndexer := makeIndexer()

	listers := kube_util.NewListerRegistry(
		&fakeClientNodeLister{client: clientset},
		&fakeClientNodeLister{client: clientset, filter: kube_util.IsNodeReadyAndSchedulable},
		&fakeClientPodLister{client: clientset, filter: func(pod *corev1.Pod) bool {
			return pod.Spec.NodeName != ""
		}},
		&fakeClientPodLister{client: clientset, filter: func(pod *corev1.Pod) bool {
			return pod.Spec.NodeName == ""
		}},
		&pdbLister{lister: v1beta1.NewPodDisruptionBudgetLister(pdbIndexer)},
		v1appslister.NewDaemonSetLister(daemonsetIndexer),
		v1corelister.NewReplicationControllerLister(replicationControllerIndexer),
		v1batchlister.NewJobLister(jobIndexer),
		v1appslister.NewReplicaSetLister(replicasetIndexer),
		v1appslister.NewStatefulSetLister(statefulsetIndexer))

	autoscalingContext := &scalercontext.AutoscalingContext{
		AutoscalingOptions: options,
		AutoscalingKubeClients: scalercontext.AutoscalingKubeClients{
			ClientSet:      clientset,
			Recorder:       recorder,
			LogRecorder:    statusRecorder,
			ListerRegistry: listers,
		},
		CloudProvider:      provider,
		PredicateChecker:   predicateChecker,
		ClusterSnapshot:    simulator.NewBasicClusterSnapshot(),
		ExpanderStrategy:   highestpriority.NewStrategy(),
		EstimatorBuilder:   estimatorBuilder,
		ProcessorCallbacks: processorCallbacks,
	}

	clusterStateConfig := clusterstate.ClusterStateRegistryConfig{
		MaxTotalUnreadyPercentage: options.MaxTotalUnreadyPercentage,
		OkTotalUnreadyCount:       options.OkTotalUnreadyCount,
		MaxNodeProvisionTime:      options.MaxNodeProvisionTime,
		DisableNodeInstancesCache: options.DisableNodeInstancesCache,
		BackoffNoFullScaleDown:    options.BackoffNoFullScaleDown,
	}

	ngBackoff := newBackoff()
	if options.BackoffNoFullScaleDown {
		ngBackoff = backoff.NewInfiniteBackoff()
	}

	clusterState := clusterstate.NewClusterStateRegistry(provider, clusterStateConfig, autoscalingContext.LogRecorder, ngBackoff)
	sd := NewScaleDown(autoscalingContext, clusterState)
	sd.runSync = true
	initialTime := time.Date(2020, 01, 01, 00, 00, 00, 0, time.UTC)

	autoscaler := &StaticAutoscaler{
		AutoscalingContext:    autoscalingContext,
		clusterStateRegistry:  clusterState,
		lastScaleUpTime:       initialTime,
		lastScaleDownFailTime: initialTime,
		scaleDown:             sd,
		processors:            NewTestProcessors(),
		processorCallbacks:    processorCallbacks,
		initialized:           true,
	}

	env := &zalandoTestEnv{
		t:                            t,
		expectedGID:                  getGID(),
		interval:                     interval,
		client:                       clientset,
		initialTime:                  initialTime,
		currentTime:                  initialTime,
		cloudProvider:                provider,
		pdbIndexer:                   pdbIndexer,
		daemonsetIndexer:             daemonsetIndexer,
		replicationControllerIndexer: replicationControllerIndexer,
		jobIndexer:                   jobIndexer,
		replicasetIndexer:            replicasetIndexer,
		statefulsetIndexer:           statefulsetIndexer,
		autoscaler:                   autoscaler,
	}

	logWriter.setTestEnvironment(env)
	defer func() {
		logWriter.setTestEnvironment(nil)
	}()

	// Override the scaledown time provider
	now = func() time.Time { return env.currentTime }
	defer func() {
		now = time.Now
	}()

	testFn(env)
}

// NewTestReplicaSet creates an example ReplicaSet object
func NewTestReplicaSet(name string, replicas int32) *appsv1.ReplicaSet {
	return &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testNamespace,
			Name:      name,
			UID:       types.UID(uuid.New().String()),
		},
		Spec: appsv1.ReplicaSetSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"replicaset-name": name},
			},
		},
	}
}

// NewReplicaSetPod creates an example Pod object owned by the provided ReplicaSet
func NewReplicaSetPod(owner *appsv1.ReplicaSet, cpu, memory resource.Quantity) *corev1.Pod {
	name := fmt.Sprintf("%s-%s", owner.Name, uuid.New().String())
	result := NewTestPod(name, cpu, memory)
	controller := true
	result.OwnerReferences = []metav1.OwnerReference{
		{
			APIVersion: "apps/v1",
			Kind:       "ReplicaSet",
			Name:       owner.Name,
			UID:        owner.UID,
			Controller: &controller,
		},
	}

	if result.Labels == nil {
		result.Labels = make(map[string]string)
	}
	for k, v := range owner.Spec.Selector.MatchLabels {
		result.Labels[k] = v
	}
	return result
}

// NewTestPod creates an example Pod object
func NewTestPod(name string, cpu, memory resource.Quantity) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testNamespace,
			Name:      name,
			UID:       types.UID(uuid.New().String()),
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  "main",
					Image: "nginx",
					Resources: corev1.ResourceRequirements{
						Limits: corev1.ResourceList{
							corev1.ResourceCPU:    cpu,
							corev1.ResourceMemory: memory,
						},
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    cpu,
							corev1.ResourceMemory: memory,
						},
					},
				},
			},
		},
	}
}

func getGID() uint64 {
	b := make([]byte, 64)
	b = b[:runtime.Stack(b, false)]
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	b = b[:bytes.IndexByte(b, ' ')]
	n, _ := strconv.ParseUint(string(b), 10, 64)
	return n
}

func ensureSameGoroutine(expected uint64) {
	currentGID := getGID()
	if currentGID != expected {
		panic(fmt.Sprintf("called from a different goroutine %d, expected %d", currentGID, expected))
	}
}
