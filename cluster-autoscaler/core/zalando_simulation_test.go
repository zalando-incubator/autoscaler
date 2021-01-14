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
	"context"
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/utils/deletetaint"
	kube_client "k8s.io/client-go/kubernetes"
	"k8s.io/klog"
)

const (
	labelScalePriority = "zalando.org/scaling-priority"
)

func TestMain(m *testing.M) {
	klog.InitFlags(flag.CommandLine)
	redirectLogging()
	err := flag.Set("v", "3")
	if err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}

func TestBrokenScalingTest(t *testing.T) {
	opts := defaultZalandoAutoscalingOptions()
	opts.BackoffNoFullScaleDown = false
	RunSimulation(t, opts, 10*time.Second, func(env *zalandoTestEnv) {
		env.AddNodeGroup("ng-fallback", 10, resource.MustParse("4"), resource.MustParse("32Gi"), nil)
		env.AddNodeGroup("ng-1", 10, resource.MustParse("4"), resource.MustParse("32Gi"), map[string]string{labelScalePriority: "100"})
		env.AddNodeGroup("ng-2", 10, resource.MustParse("4"), resource.MustParse("32Gi"), map[string]string{labelScalePriority: "110"})
		env.AddNodeGroup("ng-3", 10, resource.MustParse("4"), resource.MustParse("32Gi"), map[string]string{labelScalePriority: "120"})

		env.StepFor(10 * time.Second).ExpectNoCommands()

		env.AddPod(NewTestPod("foo", resource.MustParse("1"), resource.MustParse("24Gi")))
		env.AddPod(NewTestPod("bar", resource.MustParse("1"), resource.MustParse("24Gi")))

		env.StepUntilCommand(20*time.Hour, zalandoCloudProviderCommand{
			commandType: zalandoCloudProviderCommandIncreaseSize,
			nodeGroup:   "ng-fallback",
			delta:       2,
		})
		require.True(t, env.CurrentTime() > 60*time.Minute, "upstream autoscaler should take a lot of time to fallback")
		env.LogStatus()
	})
}

func TestZalandoScalingTest(t *testing.T) {
	opts := defaultZalandoAutoscalingOptions()
	RunSimulation(t, opts, 10*time.Second, func(env *zalandoTestEnv) {
		env.AddNodeGroup("ng-fallback", 10, resource.MustParse("4"), resource.MustParse("32Gi"), nil)
		env.AddNodeGroup("ng-1", 10, resource.MustParse("4"), resource.MustParse("32Gi"), map[string]string{labelScalePriority: "100"})
		env.AddNodeGroup("ng-2", 10, resource.MustParse("4"), resource.MustParse("32Gi"), map[string]string{labelScalePriority: "110"})
		env.AddNodeGroup("ng-3", 10, resource.MustParse("4"), resource.MustParse("32Gi"), map[string]string{labelScalePriority: "120"})

		env.StepFor(10 * time.Second).ExpectNoCommands()

		p1 := NewTestPod("foo", resource.MustParse("1"), resource.MustParse("24Gi"))
		p2 := NewTestPod("bar", resource.MustParse("1"), resource.MustParse("24Gi"))

		env.AddPod(p1)
		env.AddPod(p2)

		env.StepFor(22*time.Minute).ExpectCommands(
			// scaled up first
			zalandoCloudProviderCommand{commandType: zalandoCloudProviderCommandIncreaseSize, nodeGroup: "ng-3", delta: 2},

			// scaled up once the timeout expires for the first node group. ng-3 is still not scaled down because the incorrect size fixup code lags behind.
			zalandoCloudProviderCommand{commandType: zalandoCloudProviderCommandIncreaseSize, nodeGroup: "ng-2", delta: 2},

			// fixNodeGroupSize finally triggers (takes close to another node provisioning timeout to trigger). we still keep a sentinel node, so we expect a scale down by 1 node only.
			zalandoCloudProviderCommand{commandType: zalandoCloudProviderCommandDecreaseTargetSize, nodeGroup: "ng-3", delta: -1},

			// ng-2 times out as well
			zalandoCloudProviderCommand{commandType: zalandoCloudProviderCommandIncreaseSize, nodeGroup: "ng-1", delta: 2},

			// ng-2 is scaled down another ~7 minutes later
			zalandoCloudProviderCommand{commandType: zalandoCloudProviderCommandDecreaseTargetSize, nodeGroup: "ng-2", delta: -1},

			// ng-1 times out, so we expect ng-fallback to be tried next
			zalandoCloudProviderCommand{commandType: zalandoCloudProviderCommandIncreaseSize, nodeGroup: "ng-fallback", delta: 2},
		)
		env.AddInstance("ng-fallback", "i-1", false).AddNode("i-1", true).
			AddInstance("ng-fallback", "i-2", false).AddNode("i-2", true).
			SchedulePod(p1, "i-1").
			SchedulePod(p2, "i-2").
			StepFor(15*time.Minute).
			ExpectCommands(zalandoCloudProviderCommand{commandType: zalandoCloudProviderCommandDecreaseTargetSize, nodeGroup: "ng-1", delta: -1}).
			ExpectBackedOff("ng-1").ExpectTargetSize("ng-1", 1).
			ExpectBackedOff("ng-2").ExpectTargetSize("ng-2", 1).
			ExpectBackedOff("ng-3").ExpectTargetSize("ng-3", 1).
			ExpectNotBackedOff("ng-fallback").ExpectTargetSize("ng-fallback", 2)

		// ASG finishes scaling up
		env.AddInstance("ng-1", "i-3", false).AddNode("i-3", true).
			StepOnce().
			ExpectNotBackedOff("ng-1")

		// ASG was reset to 0 externally
		env.SetTargetSize("ng-2", 0).
			StepFor(2 * time.Minute).
			ExpectNotBackedOff("ng-2")

		env.LogStatus()
	})
}

func TestZalandoScalingTestRestartBackoff(t *testing.T) {
	opts := defaultZalandoAutoscalingOptions()
	RunSimulation(t, opts, 10*time.Second, func(env *zalandoTestEnv) {
		env.AddNodeGroup("ng-fallback", 10, resource.MustParse("4"), resource.MustParse("32Gi"), nil)
		env.AddNodeGroup("ng-1", 10, resource.MustParse("4"), resource.MustParse("32Gi"), map[string]string{labelScalePriority: "100"})

		env.SetTargetSize("ng-1", 1)

		env.AddPod(NewTestPod("foo", resource.MustParse("1"), resource.MustParse("24Gi"))).
			StepUntilCommand(20*time.Minute, zalandoCloudProviderCommand{commandType: zalandoCloudProviderCommandIncreaseSize, nodeGroup: "ng-fallback", delta: 1})
	})
}

func TestZalandoScalingTestScaleDownBackoff(t *testing.T) {
	opts := defaultZalandoAutoscalingOptions()
	RunSimulation(t, opts, 10*time.Second, func(env *zalandoTestEnv) {
		env.AddNodeGroup("ng-fallback", 10, resource.MustParse("4"), resource.MustParse("32Gi"), nil)
		env.AddNodeGroup("ng-1", 10, resource.MustParse("4"), resource.MustParse("32Gi"), map[string]string{labelScalePriority: "100"})

		pod := NewTestPod("foo", resource.MustParse("1"), resource.MustParse("24Gi"))

		env.AddPod(pod).
			StepUntilNextCommand(1*time.Minute).
			ExpectCommands(zalandoCloudProviderCommand{commandType: zalandoCloudProviderCommandIncreaseSize, nodeGroup: "ng-1", delta: 1}).
			AddInstance("ng-1", "i-1", false).
			AddNode("i-1", true).
			SchedulePod(pod, "i-1")

		pod2 := NewTestPod("bar", resource.MustParse("1"), resource.MustParse("24Gi"))
		env.AddPod(pod2).
			StepUntilNextCommand(1*time.Minute).
			StepUntilCommand(20*time.Minute, zalandoCloudProviderCommand{commandType: zalandoCloudProviderCommandIncreaseSize, nodeGroup: "ng-fallback", delta: 1}).
			ExpectCommands(
				zalandoCloudProviderCommand{commandType: zalandoCloudProviderCommandIncreaseSize, nodeGroup: "ng-1", delta: 1},
				zalandoCloudProviderCommand{commandType: zalandoCloudProviderCommandIncreaseSize, nodeGroup: "ng-fallback", delta: 1}).
			AddInstance("ng-fallback", "i-2", false).
			AddNode("i-2", true).
			SchedulePod(pod2, "i-2").
			ExpectTargetSize("ng-1", 2)

		env.RemovePod(pod).
			StepUntilNextCommand(20*time.Minute).
			ExpectCommands(zalandoCloudProviderCommand{commandType: zalandoCloudProviderCommandDeleteNodes, nodeGroup: "ng-1", nodeNames: []string{"i-1"}}).
			RemoveInstance("i-1", false).
			RemoveNode("i-1")

		env.StepFor(1*time.Hour).
			ExpectNoCommands().
			ExpectTargetSize("ng-1", 1).
			ExpectBackedOff("ng-1")
	})
}

func TestMaxSizeIncrease(t *testing.T) {
	opts := defaultZalandoAutoscalingOptions()
	RunSimulation(t, opts, 10*time.Second, func(env *zalandoTestEnv) {
		env.AddNodeGroup("ng-1", 1, resource.MustParse("4"), resource.MustParse("32Gi"), nil)

		pod := NewTestPod("foo", resource.MustParse("1"), resource.MustParse("24Gi"))

		env.AddPod(pod).
			StepOnce().
			ExpectCommands(zalandoCloudProviderCommand{commandType: zalandoCloudProviderCommandIncreaseSize, nodeGroup: "ng-1", delta: 1}).
			AddInstance("ng-1", "i-1", false).
			AddNode("i-1", true).
			SchedulePod(pod, "i-1")

		pod2 := NewTestPod("bar", resource.MustParse("1"), resource.MustParse("24Gi"))
		env.AddPod(pod2).
			StepFor(10 * time.Minute).
			ExpectNoCommands()

		env.SetMaxSize("ng-1", 2).
			StepOnce().
			ExpectCommands(zalandoCloudProviderCommand{commandType: zalandoCloudProviderCommandIncreaseSize, nodeGroup: "ng-1", delta: 1})
	})
}

func TestTemplateNodeChange(t *testing.T) {
	opts := defaultZalandoAutoscalingOptions()
	RunSimulation(t, opts, 10*time.Second, func(env *zalandoTestEnv) {
		env.AddNodeGroup("ng-1", 10, resource.MustParse("4"), resource.MustParse("32Gi"), nil)

		pod := NewTestPod("foo", resource.MustParse("1"), resource.MustParse("24Gi"))

		env.AddPod(pod).
			StepOnce().
			ExpectCommands(zalandoCloudProviderCommand{commandType: zalandoCloudProviderCommandIncreaseSize, nodeGroup: "ng-1", delta: 1}).
			AddInstance("ng-1", "i-1", false).
			AddNode("i-1", true).
			SchedulePod(pod, "i-1")

		pod2 := NewTestPod("bar", resource.MustParse("1"), resource.MustParse("48Gi"))
		env.AddPod(pod2).
			StepFor(10 * time.Minute).
			ExpectNoCommands()

		env.SetTemplateNode("ng-1", resource.MustParse("4"), resource.MustParse("64Gi"), nil).
			StepOnce().
			ExpectCommands(zalandoCloudProviderCommand{commandType: zalandoCloudProviderCommandIncreaseSize, nodeGroup: "ng-1", delta: 1})
	})
}

func TestMaxSizeDecrease(t *testing.T) {
	opts := defaultZalandoAutoscalingOptions()
	RunSimulation(t, opts, 10*time.Second, func(env *zalandoTestEnv) {
		env.AddNodeGroup("ng-1", 10, resource.MustParse("4"), resource.MustParse("32Gi"), nil)

		pod := NewTestPod("foo", resource.MustParse("1"), resource.MustParse("24Gi"))

		env.AddPod(pod).
			StepOnce().
			ExpectCommands(zalandoCloudProviderCommand{commandType: zalandoCloudProviderCommandIncreaseSize, nodeGroup: "ng-1", delta: 1}).
			AddInstance("ng-1", "i-1", false).
			AddNode("i-1", true).
			SchedulePod(pod, "i-1")

		pod2 := NewTestPod("bar", resource.MustParse("1"), resource.MustParse("24Gi"))
		env.SetMaxSize("ng-1", 1).
			AddPod(pod2).
			StepFor(10 * time.Minute).
			ExpectNoCommands()
	})
}

func TestAddNodeGroup(t *testing.T) {
	opts := defaultZalandoAutoscalingOptions()
	RunSimulation(t, opts, 10*time.Second, func(env *zalandoTestEnv) {
		env.AddNodeGroup("ng-1", 10, resource.MustParse("4"), resource.MustParse("32Gi"), nil)

		pod := NewTestPod("foo", resource.MustParse("1"), resource.MustParse("32Gi"))

		env.AddPod(pod).
			StepOnce().
			ExpectCommands(zalandoCloudProviderCommand{commandType: zalandoCloudProviderCommandIncreaseSize, nodeGroup: "ng-1", delta: 1}).
			AddInstance("ng-1", "i-1", false).
			AddNode("i-1", true).
			SchedulePod(pod, "i-1")

		pod2 := NewTestPod("bar", resource.MustParse("1"), resource.MustParse("64Gi"))
		env.AddPod(pod2).
			StepFor(10 * time.Minute).
			ExpectNoCommands()

		env.AddNodeGroup("ng-2", 10, resource.MustParse("4"), resource.MustParse("64Gi"), nil).
			StepOnce().
			ExpectCommands(zalandoCloudProviderCommand{commandType: zalandoCloudProviderCommandIncreaseSize, nodeGroup: "ng-2", delta: 1})
	})
}

func TestRemoveNodeGroup(t *testing.T) {
	opts := defaultZalandoAutoscalingOptions()
	RunSimulation(t, opts, 10*time.Second, func(env *zalandoTestEnv) {
		env.AddNodeGroup("ng-1", 10, resource.MustParse("4"), resource.MustParse("32Gi"), map[string]string{labelScalePriority: "110"}).
			AddNodeGroup("ng-2", 10, resource.MustParse("4"), resource.MustParse("64Gi"), map[string]string{labelScalePriority: "100"})

		pod := NewTestPod("foo", resource.MustParse("1"), resource.MustParse("32Gi"))

		env.AddPod(pod).
			StepOnce().
			ExpectCommands(zalandoCloudProviderCommand{commandType: zalandoCloudProviderCommandIncreaseSize, nodeGroup: "ng-1", delta: 1}).
			AddInstance("ng-1", "i-1", false).
			AddNode("i-1", true).
			SchedulePod(pod, "i-1")

		pod2 := NewTestPod("bar", resource.MustParse("1"), resource.MustParse("64Gi"))
		env.RemoveNodeGroup("ng-2").
			AddPod(pod2).
			StepFor(10 * time.Minute).
			ExpectNoCommands()
	})
}

func TestSnapshotBug(t *testing.T) {
	// This fails in the upstream version because NodeInfo.Clone() is bugged and doesn't clone the underlying Node object,
	// leading to things like scale_up.go:buildNodeInfoForNodeTemplate() corrupting existing state (and possibly more).

	opts := defaultZalandoAutoscalingOptions()
	RunSimulation(t, opts, 10*time.Second, func(env *zalandoTestEnv) {
		env.AddNodeGroup("ng-1", 10, resource.MustParse("4"), resource.MustParse("32Gi"), nil).
			AddNodeGroup("ng-2", 10, resource.MustParse("4"), resource.MustParse("32Gi"), nil).
			AddNodeGroup("ng-3", 10, resource.MustParse("4"), resource.MustParse("32Gi"), nil)

		for i := 0; i < 5; i++ {
			env.AddPod(NewTestPod(fmt.Sprintf("foo-%d", i), resource.MustParse("1"), resource.MustParse("20Gi")))
		}

		env.StepFor(30*time.Second).
			AddInstance("ng-1", "i-1", false).
			AddInstance("ng-1", "i-2", false).
			AddInstance("ng-2", "i-3", false).
			AddInstance("ng-2", "i-4", false).
			AddInstance("ng-3", "i-5", false)

		for i := 5; i < 10; i++ {
			env.AddPod(NewTestPod(fmt.Sprintf("foo-%d", i), resource.MustParse("1"), resource.MustParse("20Gi")))
		}

		env.StepFor(2 * time.Minute)
	})
}

func TestBrokenAZSplit(t *testing.T) {
	for _, tc := range []struct {
		name             string
		splitFactor      int
		initialInstances map[string]int
		remainingPods    int
		expectedScaleUp  int
	}{
		{
			name:        "completely skewed, broken upstream behaviour",
			splitFactor: 0,
			initialInstances: map[string]int{
				"ng-1": 3,
				"ng-2": 3,
				"ng-3": 1,
			},
			remainingPods:   13,
			expectedScaleUp: 23,
		},
		{
			name:        "completely skewed, split enabled",
			splitFactor: 3,
			initialInstances: map[string]int{
				"ng-1": 3,
				"ng-2": 3,
				"ng-3": 1,
			},
			remainingPods:   13,
			expectedScaleUp: 13,
		},
		{
			name:        "partially skewed, broken upstream behaviour",
			splitFactor: 0,
			initialInstances: map[string]int{
				"ng-1": 5,
				"ng-2": 1,
				"ng-3": 1,
			},
			remainingPods:   13,
			expectedScaleUp: 16,
		},
		{
			name:        "partially skewed, split enabled",
			splitFactor: 3,
			initialInstances: map[string]int{
				"ng-1": 5,
				"ng-2": 1,
				"ng-3": 1,
			},
			remainingPods:   13,
			expectedScaleUp: 13,
		},
		{
			name:        "no skew, upstream behaviour",
			splitFactor: 0,
			initialInstances: map[string]int{
				"ng-1": 2,
				"ng-2": 2,
				"ng-3": 2,
			},
			remainingPods:   14,
			expectedScaleUp: 14,
		},
		{
			name:        "no skew, split enabled",
			splitFactor: 3,
			initialInstances: map[string]int{
				"ng-1": 2,
				"ng-2": 2,
				"ng-3": 2,
			},
			remainingPods:   14,
			expectedScaleUp: 14,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := defaultZalandoAutoscalingOptions()
			opts.TopologySpreadConstraintSplitFactor = tc.splitFactor

			RunSimulation(t, opts, 10*time.Second, func(env *zalandoTestEnv) {
				env.AddNodeGroup("ng-1", 100, resource.MustParse("4"), resource.MustParse("32Gi"), map[string]string{corev1.LabelZoneFailureDomainStable: "A"}).
					AddNodeGroup("ng-2", 100, resource.MustParse("4"), resource.MustParse("32Gi"), map[string]string{corev1.LabelZoneFailureDomainStable: "B"}).
					AddNodeGroup("ng-3", 100, resource.MustParse("4"), resource.MustParse("32Gi"), map[string]string{corev1.LabelZoneFailureDomainStable: "C"})

				rs := NewTestReplicaSet("foo", 20)

				newPod := func() *corev1.Pod {
					pod := NewReplicaSetPod(rs, resource.MustParse("3"), resource.MustParse("30Gi"))
					pod.Spec.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{
						{
							MaxSkew:           1,
							TopologyKey:       corev1.LabelZoneFailureDomainStable,
							WhenUnsatisfiable: corev1.DoNotSchedule,
							LabelSelector:     rs.Spec.Selector,
						},
					}
					return pod
				}

				// Setup the initial state
				for ng, instances := range tc.initialInstances {
					for i := 0; i < instances; i++ {
						instanceId := fmt.Sprintf("i-%s-%d", ng, i)
						env.AddInstance(ng, instanceId, true).AddNode(instanceId, true).
							AddScheduledPod(newPod(), instanceId)
					}
				}

				env.StepFor(1 * time.Minute).ExpectNoCommands()

				for i := 0; i < tc.remainingPods; i++ {
					env.AddPod(newPod())
				}

				env.StepFor(5 * time.Minute)

				totalScaleUp := 0
				for _, command := range env.ConsumeCommands() {
					require.Equal(t, zalandoCloudProviderCommandIncreaseSize, command.commandType)
					totalScaleUp += command.delta
				}
				require.Equal(t, tc.expectedScaleUp, totalScaleUp)
			})
		})
	}
}

func TestScaleDownContinuousScaleUp(t *testing.T) {
	opts := defaultZalandoAutoscalingOptions()

	// So we don't need to deal with scale-up timeouts
	opts.MaxNodeProvisionTime = 30 * time.Minute

	RunSimulation(t, opts, 10*time.Second, func(env *zalandoTestEnv) {
		env.AddNodeGroup("ng-1", 10, resource.MustParse("1"), resource.MustParse("8Gi"), map[string]string{"id": "ng-1"}).
			AddNodeGroup("ng-2", 10, resource.MustParse("4"), resource.MustParse("32Gi"), map[string]string{"id": "ng-2"}).
			AddNodeGroup("ng-3", 10, resource.MustParse("4"), resource.MustParse("32Gi"), map[string]string{"id": "ng-3"})

		// Add one instance in both ng-1 and ng-2
		env.AddInstance("ng-1", "i-1", true).AddNode("i-1", true).
			AddInstance("ng-2", "i-2", true).AddNode("i-2", true).
			StepOnce()

		// Fast forward close to the scale-down time for the nodes in ng-1 and ng-2
		env.StepFor(9 * time.Minute)

		// Add a pod that will trigger a scale-up of ng-3 and a pod that will be schedulable on the node in ng-2
		ng3Pod := NewTestPod("pod-ng3", resource.MustParse("3"), resource.MustParse("10Gi"))
		ng3Pod.Spec.NodeSelector = map[string]string{"id": "ng-3"}

		ng2Pod := NewTestPod("pod-ng2", resource.MustParse("3"), resource.MustParse("10Gi"))
		ng2Pod.Spec.NodeSelector = map[string]string{"id": "ng-2"}

		env.AddPod(ng2Pod).AddPod(ng3Pod)

		env.StepOnce().
			ExpectCommands(zalandoCloudProviderCommand{commandType: zalandoCloudProviderCommandIncreaseSize, nodeGroup: "ng-3", delta: 1})

		// Run for one more minute; the node in ng-1 should be scaled down but the node in ng-2 should be kept
		env.StepFor(1*time.Minute).
			ExpectCommands(zalandoCloudProviderCommand{commandType: zalandoCloudProviderCommandDeleteNodes, nodeGroup: "ng-1", nodeNames: []string{"i-1"}}).
			RemoveInstance("i-1", false).
			RemoveNode("i-1")

		env.StepFor(20 * time.Minute).ExpectNoCommands()
	})
}

func TestDeleteTaintScaleUpDraining(t *testing.T) {
	opts := defaultZalandoAutoscalingOptions()

	RunSimulation(t, opts, 10*time.Second, func(env *zalandoTestEnv) {
		env.AddNodeGroup("ng-1", 10, resource.MustParse("1"), resource.MustParse("8Gi"), nil)

		// Add an existing instance
		env.AddInstance("ng-1", "i-1", true).AddNode("i-1", true).
			StepOnce()

		// Manually mark the node with the delete taint (CA won't reset it)
		node, err := env.client.CoreV1().Nodes().Get(context.Background(), "i-1", metav1.GetOptions{})
		require.NoError(t, err)

		err = deletetaint.MarkToBeDeleted(node, env.client)
		require.NoError(t, err)
		env.StepOnce()

		// Add a pod, this should trigger a scale-up. Upstream will erroneously consider this node as upcoming.
		env.AddPod(NewTestPod("foo", resource.MustParse("1"), resource.MustParse("4Gi"))).
			StepOnce().
			ExpectCommands(zalandoCloudProviderCommand{commandType: zalandoCloudProviderCommandIncreaseSize, nodeGroup: "ng-1", delta: 1})
	})
}

func TestDeleteTaintScaleUpDeleting(t *testing.T) {
	opts := defaultZalandoAutoscalingOptions()

	RunSimulation(t, opts, 10*time.Second, func(env *zalandoTestEnv) {
		env.AddNodeGroup("ng-1", 10, resource.MustParse("1"), resource.MustParse("8Gi"), nil)

		// Add an existing instance
		env.AddInstance("ng-1", "i-1", true).AddNode("i-1", true).
			StepOnce()

		// Manually mark the node with the delete taint and the 'being deleted' taint (CA won't reset it)
		for _, fn := range []func(*corev1.Node, kube_client.Interface) error{
			deletetaint.MarkToBeDeleted,
			deletetaint.MarkBeingDeleted,
		} {
			node, err := env.client.CoreV1().Nodes().Get(context.Background(), "i-1", metav1.GetOptions{})
			require.NoError(t, err)

			err = fn(node, env.client)
			require.NoError(t, err)
		}

		// Emulate deleting the instance on the cloud provider side (but keep it on Kubernetes side)
		env.RemoveInstance("i-1", true).
			StepOnce()

		// Add a pod, this should trigger a scale-up. Upstream will erroneously consider this node as upcoming.
		env.AddPod(NewTestPod("foo", resource.MustParse("1"), resource.MustParse("4Gi"))).
			StepOnce().
			ExpectCommands(zalandoCloudProviderCommand{commandType: zalandoCloudProviderCommandIncreaseSize, nodeGroup: "ng-1", delta: 1})
	})
}

func TestNodeNotReadyCustomTaint(t *testing.T) {
	opts := defaultZalandoAutoscalingOptions()

	RunSimulation(t, opts, 10*time.Second, func(env *zalandoTestEnv) {
		env.AddNodeGroup("ng-1", 10, resource.MustParse("1"), resource.MustParse("8Gi"), nil).
			AddNodeGroup("ng-2", 10, resource.MustParse("1"), resource.MustParse("8Gi"), map[string]string{labelScalePriority: "100"})

		pod := NewTestPod("foo", resource.MustParse("1"), resource.MustParse("4Gi"))
		env.AddPod(pod).
			StepOnce().
			ExpectCommands(zalandoCloudProviderCommand{commandType: zalandoCloudProviderCommandIncreaseSize, nodeGroup: "ng-2", delta: 1})

		// Add a ready node
		env.AddInstance("ng-2", "i-1", false).
			AddNode("i-1", true)

		// Mark the instance "not-ready" via the `zalando.org/node-not-ready` taint
		node, err := env.client.CoreV1().Nodes().Get(context.Background(), "i-1", metav1.GetOptions{})
		require.NoError(t, err)

		node.Spec.Taints = append(node.Spec.Taints, corev1.Taint{
			Key:    "zalando.org/node-not-ready",
			Effect: corev1.TaintEffectNoSchedule,
		})
		_, err = env.client.CoreV1().Nodes().Update(context.Background(), node, metav1.UpdateOptions{})
		require.NoError(t, err)

		// When the node is not ready for ~7 minutes we expect a scaleup of another node.
		env.StepFor(opts.MaxNodeProvisionTime).
			ExpectNoCommands().
			StepOnce().
			ExpectCommands(zalandoCloudProviderCommand{commandType: zalandoCloudProviderCommandIncreaseSize, nodeGroup: "ng-1", delta: 1})

		// Add a node and schedule the pod
		env.AddInstance("ng-1", "i-2", false).
			AddNode("i-2", true).
			SchedulePod(pod, "i-2")

		// The problematic node should be decommissioned after the timeout expires
		env.StepFor(opts.ScaleDownUnreadyTime - opts.MaxNodeProvisionTime).
			ExpectNoCommands().
			StepOnce().
			ExpectCommands(zalandoCloudProviderCommand{commandType: zalandoCloudProviderCommandDeleteNodes, nodeGroup: "ng-2", nodeNames: []string{"i-1"}})
	})
}
