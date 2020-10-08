package core

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1beta1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/clusterstate"
	"k8s.io/autoscaler/cluster-autoscaler/clusterstate/utils"
	"k8s.io/autoscaler/cluster-autoscaler/config"
	scalercontext "k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/estimator"
	"k8s.io/autoscaler/cluster-autoscaler/expander"
	"k8s.io/autoscaler/cluster-autoscaler/expander/highestpriority"
	"k8s.io/autoscaler/cluster-autoscaler/simulator"
	"k8s.io/autoscaler/cluster-autoscaler/utils/deletetaint"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	kube_util "k8s.io/autoscaler/cluster-autoscaler/utils/kubernetes"
	"k8s.io/autoscaler/cluster-autoscaler/utils/units"
	"k8s.io/client-go/kubernetes/fake"
	v1appslister "k8s.io/client-go/listers/apps/v1"
	v1batchlister "k8s.io/client-go/listers/batch/v1"
	v1corelister "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/listers/policy/v1beta1"
	"k8s.io/client-go/tools/cache"
	kube_record "k8s.io/client-go/tools/record"
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

type zalandoCloudProviderCommand struct {
	commandType zalandoCloudProviderCommandType
	nodeGroup   string
	delta       int      // for increase/decrease size commands
	nodeNames   []string // for deleteNodes
}

type zalandoTestCloudProvider struct {
	limiter     *cloudprovider.ResourceLimiter
	expectedGID uint64
	nodeGroups  []*zalandoTestCloudProviderNodeGroup
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

	var result []cloudprovider.Instance
	for instance, _ := range g.instances {
		result = append(result, cloudprovider.Instance{
			Id: fmt.Sprintf("zalando-test:///%s/%s", g.id, instance),
		})
	}
	for i := 0; i < g.targetSize-len(g.instances); i++ {
		result = append(result, cloudprovider.Instance{
			Id: fmt.Sprintf("zalando-test:///%s/i-placeholder-%d", g.id, i),
		})
	}
	return result, nil
}

func (g *zalandoTestCloudProviderNodeGroup) TemplateNodeInfo() (*schedulernodeinfo.NodeInfo, error) {
	ensureSameGoroutine(g.expectedGID)

	return g.templateNode.Clone(), nil
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

		// customized
		ExpanderName:                     expander.HighestPriorityExpanderName,
		ExpendablePodsPriorityCutoff:     -1000000,
		ScaleDownEnabled:                 true,
		ScaleDownDelayAfterAdd:           -1 * time.Second,
		ScaleDownUnneededTime:            10 * time.Minute,
		ScaleDownUtilizationThreshold:    1.0,
		BalanceSimilarNodeGroups:         true,
		MaxNodeProvisionTime:             7 * time.Minute,
		MaxNodesTotal:                    100,
		ScaleUpTemplateFromCloudProvider: true,
	}
}

type zalandoTestEnv struct {
	t                            *testing.T
	expectedGID                  uint64
	interval                     time.Duration
	client                       *fake.Clientset
	initialTime                  time.Time
	currentTime                  time.Time
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

type simulationLogWriter struct {
	testEnv *zalandoTestEnv
}

func (w *simulationLogWriter) Write(p []byte) (int, error) {
	s := fmt.Sprintf("[%8s] %s", w.testEnv.currentTime.Sub(w.testEnv.initialTime), string(p))
	_, err := os.Stderr.Write([]byte(s))
	if err != nil {
		return 0, err
	}
	return len(p), nil
}

func (e *zalandoTestEnv) AddNodeGroup(id string, maxSize int, nodeCPU, nodeMemory resource.Quantity, nodeLabels map[string]string) *zalandoTestEnv {
	for _, group := range e.cloudProvider.nodeGroups {
		if group.id == id {
			e.t.Errorf("node group already exists: %s", id)
		}
	}

	templateNode := schedulernodeinfo.NewNodeInfo()
	err := templateNode.SetNode(&corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Labels: nodeLabels,
		},
		Status: corev1.NodeStatus{
			Capacity: corev1.ResourceList{
				corev1.ResourceCPU:    nodeCPU,
				corev1.ResourceMemory: nodeMemory,
				corev1.ResourcePods:   resource.MustParse("110"),
			},
			Allocatable: corev1.ResourceList{
				corev1.ResourceCPU:    nodeCPU,
				corev1.ResourceMemory: nodeMemory,
				corev1.ResourcePods:   resource.MustParse("110"),
			},
		},
	})
	require.NoError(e.t, err)

	ng := &zalandoTestCloudProviderNodeGroup{
		id:            id,
		expectedGID:   e.expectedGID,
		maxSize:       maxSize,
		instances:     sets.NewString(),
		templateNode:  templateNode,
		handleCommand: e.handleCommand,
	}
	e.cloudProvider.nodeGroups = append(e.cloudProvider.nodeGroups, ng)
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
	return e
}

func (e *zalandoTestEnv) AddPod(pod *corev1.Pod) *zalandoTestEnv {
	_, err := e.client.CoreV1().Pods(pod.Namespace).Create(context.Background(), pod, metav1.CreateOptions{})
	require.NoError(e.t, err)
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
	e.currentTime = e.currentTime.Add(e.interval)

	// This is usually running asynchronously, we have to emulate it instead
	for _, group := range e.cloudProvider.NodeGroups() {
		e.autoscaler.clusterStateRegistry.InvalidateNodeInstancesCacheEntry(group)
	}

	err := e.autoscaler.RunOnce(e.currentTime)
	require.NoError(e.t, err)
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
	switch command.commandType {
	case zalandoCloudProviderCommandIncreaseSize:
		ng, err := e.cloudProvider.nodeGroup(command.nodeGroup)
		require.NoError(e.t, err)
		require.True(e.t, command.delta >= 0, "attempted to increase size of %s with invalid delta: %d", command.nodeGroup, command.delta)
		require.True(e.t, command.delta >= 0, "attempted to increase size of %s beyond max: current %d, delta %d", command.nodeGroup, ng.targetSize, command.delta)

		ng.targetSize += command.delta
	case zalandoCloudProviderCommandDecreaseTargetSize:
		require.FailNow(e.t, "decrease target size unsupported for now")
	case zalandoCloudProviderCommandDeleteNodes:
		ng, err := e.cloudProvider.nodeGroup(command.nodeGroup)
		require.NoError(e.t, err)

		for _, name := range command.nodeNames {
			require.True(e.t, ng.instances.Has(name), "instance not found in %s: %s", command.nodeGroup, name)
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

	ng.instances.Insert(instanceId)
	if incrementTargetSize {
		ng.targetSize++

	}
	return e
}

func (e *zalandoTestEnv) AddNode(instanceId string, ready bool) *zalandoTestEnv {
	for _, group := range e.cloudProvider.nodeGroups {
		if group.instances.Has(instanceId) {
			node := group.templateNode.Node().DeepCopy()
			node.CreationTimestamp = metav1.Time{Time: e.currentTime}
			node.Name = instanceId
			node.Spec.ProviderID = fmt.Sprintf("zalando-test:///%s/%s", group.id, instanceId)

			readyStatus := corev1.ConditionTrue
			if !ready {
				readyStatus = corev1.ConditionFalse
			}
			node.Status.Conditions = []corev1.NodeCondition{
				{
					Type:   corev1.NodeReady,
					Status: readyStatus,
				},
			}
			_, err := e.client.CoreV1().Nodes().Create(context.Background(), node, metav1.CreateOptions{})
			require.NoError(e.t, err)
			return e
		}
	}

	require.FailNowf(e.t, "invalid instance", "instance %s doesn't belong to any node groups", instanceId)
	return e
}

func (e *zalandoTestEnv) SchedulePod(podName string, nodeName string) *zalandoTestEnv {
	_, err := e.client.CoreV1().Nodes().Get(context.Background(), nodeName, metav1.GetOptions{})
	require.NoError(e.t, err, "unknown node: %s", nodeName)

	pod, err := e.client.CoreV1().Pods(testNamespace).Get(context.Background(), podName, metav1.GetOptions{})
	require.NoError(e.t, err)

	require.Empty(e.t, pod.Spec.NodeName, "pod already scheduled on %s", pod.Spec.NodeName)
	pod.Spec.NodeName = nodeName
	_, err = e.client.CoreV1().Pods(testNamespace).Update(context.Background(), pod, metav1.UpdateOptions{})
	require.NoError(e.t, err)
	return e
}

func (e *zalandoTestEnv) RemovePod(podName string) *zalandoTestEnv {
	err := e.client.CoreV1().Pods(testNamespace).Delete(context.Background(), podName, metav1.DeleteOptions{})
	require.NoError(e.t, err)

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
			err = e.client.CoreV1().Pods(item.Namespace).Delete(context.Background(), item.Name, metav1.DeleteOptions{})
			require.NoError(e.t, err)
		}
	}

	// Delete the instance and decrease the target size
	zalandoNodeGroup.instances.Delete(name)
	if decrementTargetSize {
		zalandoNodeGroup.targetSize--
	}
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

func RunSimulation(t *testing.T, options config.AutoscalingOptions, interval time.Duration, testFn func(env *zalandoTestEnv)) {
	processorCallbacks := newStaticAutoscalerProcessorCallbacks()
	clientset := fake.NewSimpleClientset()

	provider := newZalandoTestCloudProvider(scalercontext.NewResourceLimiterFromAutoscalingOptions(options), getGID())

	recorder := kube_record.NewFakeRecorder(1000)
	statusRecorder, err := utils.NewStatusMapRecorder(clientset, "kube-system", recorder, false)
	require.NoError(t, err)

	estimatorBuilder, err := estimator.NewEstimatorBuilder(options.EstimatorName)
	require.NoError(t, err)

	predicateChecker, err := simulator.NewSchedulerBasedPredicateChecker(clientset, make(chan struct{}))
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
			Recorder:       kube_record.NewFakeRecorder(1000),
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
	require.NoError(t, err)

	clusterStateConfig := clusterstate.ClusterStateRegistryConfig{
		MaxTotalUnreadyPercentage: options.MaxTotalUnreadyPercentage,
		OkTotalUnreadyCount:       options.OkTotalUnreadyCount,
		MaxNodeProvisionTime:      options.MaxNodeProvisionTime,
		RunSynchronously:          true,
	}
	clusterState := clusterstate.NewClusterStateRegistry(provider, clusterStateConfig, autoscalingContext.LogRecorder, newBackoff())
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

	// Steal the logging
	err = flag.Set("logtostderr", "false")
	if err != nil {
		panic(err)
	}
	defer func() {
		err := flag.Set("logtostderr", "true")
		if err != nil {
			panic(err)
		}
	}()

	klog.SetOutput(&simulationLogWriter{testEnv: env})
	defer func() {
		klog.SetOutput(nil)
	}()

	// Override the scaledown time provider
	now = func() time.Time { return env.currentTime }
	defer func() {
		now = time.Now
	}()

	testFn(env)
}

func NewTestReplicaSet(name string, replicas int32) *appsv1.ReplicaSet {
	return &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testNamespace,
			Name:      name,
			UID:       types.UID(uuid.New().String()),
		},
		Spec: appsv1.ReplicaSetSpec{
			Replicas: &replicas,
		},
	}
}

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
	return result
}

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
