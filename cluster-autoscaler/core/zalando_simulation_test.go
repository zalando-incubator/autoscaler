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
	"math"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/autoscaler/cluster-autoscaler/utils/deletetaint"
	kube_client "k8s.io/client-go/kubernetes"
	klog "k8s.io/klog/v2"
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

		env.AddPod(env.NewTestPod("foo", resource.MustParse("1"), resource.MustParse("24Gi")))
		env.AddPod(env.NewTestPod("bar", resource.MustParse("1"), resource.MustParse("24Gi")))

		env.StepUntilCommand(20*time.Hour, zalandoTestEnvironmentCommand{
			commandType: zalandoTestEnvironmentCommandIncreaseSize,
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

		p1 := env.NewTestPod("foo", resource.MustParse("1"), resource.MustParse("24Gi"))
		p2 := env.NewTestPod("bar", resource.MustParse("1"), resource.MustParse("24Gi"))

		env.AddPod(p1)
		env.AddPod(p2)

		env.StepFor(22*time.Minute).ExpectCommands(
			// scaled up first
			zalandoTestEnvironmentCommand{commandType: zalandoTestEnvironmentCommandIncreaseSize, nodeGroup: "ng-3", delta: 2},

			// scaled up once the timeout expires for the first node group. ng-3 is still not scaled down because the incorrect size fixup code lags behind.
			zalandoTestEnvironmentCommand{commandType: zalandoTestEnvironmentCommandIncreaseSize, nodeGroup: "ng-2", delta: 2},

			// fixNodeGroupSize finally triggers (takes close to another node provisioning timeout to trigger). we still keep a sentinel node, so we expect a scale down by 1 node only.
			zalandoTestEnvironmentCommand{commandType: zalandoTestEnvironmentCommandDecreaseTargetSize, nodeGroup: "ng-3", delta: -1},

			// ng-2 times out as well
			zalandoTestEnvironmentCommand{commandType: zalandoTestEnvironmentCommandIncreaseSize, nodeGroup: "ng-1", delta: 2},

			// ng-2 is scaled down another ~7 minutes later
			zalandoTestEnvironmentCommand{commandType: zalandoTestEnvironmentCommandDecreaseTargetSize, nodeGroup: "ng-2", delta: -1},

			// ng-1 times out, so we expect ng-fallback to be tried next
			zalandoTestEnvironmentCommand{commandType: zalandoTestEnvironmentCommandIncreaseSize, nodeGroup: "ng-fallback", delta: 2},
		)
		env.AddInstance("ng-fallback", "i-1", false).AddNode("i-1", true).
			AddInstance("ng-fallback", "i-2", false).AddNode("i-2", true).
			SchedulePod(p1, "i-1").
			SchedulePod(p2, "i-2").
			StepFor(15*time.Minute).
			ExpectCommands(zalandoTestEnvironmentCommand{commandType: zalandoTestEnvironmentCommandDecreaseTargetSize, nodeGroup: "ng-1", delta: -1}).
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

		env.AddPod(env.NewTestPod("foo", resource.MustParse("1"), resource.MustParse("24Gi"))).
			StepUntilCommand(20*time.Minute, zalandoTestEnvironmentCommand{commandType: zalandoTestEnvironmentCommandIncreaseSize, nodeGroup: "ng-fallback", delta: 1})
	})
}

func TestZalandoScalingTestScaleDownBackoff(t *testing.T) {
	opts := defaultZalandoAutoscalingOptions()
	RunSimulation(t, opts, 10*time.Second, func(env *zalandoTestEnv) {
		env.AddNodeGroup("ng-fallback", 10, resource.MustParse("4"), resource.MustParse("32Gi"), nil)
		env.AddNodeGroup("ng-1", 10, resource.MustParse("4"), resource.MustParse("32Gi"), map[string]string{labelScalePriority: "100"})

		pod := env.NewTestPod("foo", resource.MustParse("1"), resource.MustParse("24Gi"))

		env.AddPod(pod).
			StepUntilNextCommand(1*time.Minute).
			ExpectCommands(zalandoTestEnvironmentCommand{commandType: zalandoTestEnvironmentCommandIncreaseSize, nodeGroup: "ng-1", delta: 1}).
			AddInstance("ng-1", "i-1", false).
			AddNode("i-1", true).
			SchedulePod(pod, "i-1")

		pod2 := env.NewTestPod("bar", resource.MustParse("1"), resource.MustParse("24Gi"))
		env.AddPod(pod2).
			StepUntilNextCommand(1*time.Minute).
			StepUntilCommand(20*time.Minute, zalandoTestEnvironmentCommand{commandType: zalandoTestEnvironmentCommandIncreaseSize, nodeGroup: "ng-fallback", delta: 1}).
			ExpectCommands(
				zalandoTestEnvironmentCommand{commandType: zalandoTestEnvironmentCommandIncreaseSize, nodeGroup: "ng-1", delta: 1},
				zalandoTestEnvironmentCommand{commandType: zalandoTestEnvironmentCommandIncreaseSize, nodeGroup: "ng-fallback", delta: 1}).
			AddInstance("ng-fallback", "i-2", false).
			AddNode("i-2", true).
			SchedulePod(pod2, "i-2").
			ExpectTargetSize("ng-1", 2)

		env.RemovePod(pod).
			StepUntilNextCommand(20*time.Minute).
			ExpectCommands(zalandoTestEnvironmentCommand{commandType: zalandoTestEnvironmentCommandDeleteNodes, nodeGroup: "ng-1", nodeNames: []string{"i-1"}}).
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

		pod := env.NewTestPod("foo", resource.MustParse("1"), resource.MustParse("24Gi"))

		env.AddPod(pod).
			StepOnce().
			ExpectCommands(zalandoTestEnvironmentCommand{commandType: zalandoTestEnvironmentCommandIncreaseSize, nodeGroup: "ng-1", delta: 1}).
			AddInstance("ng-1", "i-1", false).
			AddNode("i-1", true).
			SchedulePod(pod, "i-1")

		pod2 := env.NewTestPod("bar", resource.MustParse("1"), resource.MustParse("24Gi"))
		env.AddPod(pod2).
			StepFor(10 * time.Minute).
			ExpectNoCommands()

		env.SetMaxSize("ng-1", 2).
			StepOnce().
			ExpectCommands(zalandoTestEnvironmentCommand{commandType: zalandoTestEnvironmentCommandIncreaseSize, nodeGroup: "ng-1", delta: 1})
	})
}

func TestTemplateNodeChange(t *testing.T) {
	opts := defaultZalandoAutoscalingOptions()
	RunSimulation(t, opts, 10*time.Second, func(env *zalandoTestEnv) {
		env.AddNodeGroup("ng-1", 10, resource.MustParse("4"), resource.MustParse("32Gi"), nil)

		pod := env.NewTestPod("foo", resource.MustParse("1"), resource.MustParse("24Gi"))

		env.AddPod(pod).
			StepOnce().
			ExpectCommands(zalandoTestEnvironmentCommand{commandType: zalandoTestEnvironmentCommandIncreaseSize, nodeGroup: "ng-1", delta: 1}).
			AddInstance("ng-1", "i-1", false).
			AddNode("i-1", true).
			SchedulePod(pod, "i-1")

		pod2 := env.NewTestPod("bar", resource.MustParse("1"), resource.MustParse("48Gi"))
		env.AddPod(pod2).
			StepFor(10 * time.Minute).
			ExpectNoCommands()

		env.SetTemplateNode("ng-1", resource.MustParse("4"), resource.MustParse("64Gi"), nil).
			StepOnce().
			ExpectCommands(zalandoTestEnvironmentCommand{commandType: zalandoTestEnvironmentCommandIncreaseSize, nodeGroup: "ng-1", delta: 1})
	})
}

func TestMaxSizeDecrease(t *testing.T) {
	opts := defaultZalandoAutoscalingOptions()
	RunSimulation(t, opts, 10*time.Second, func(env *zalandoTestEnv) {
		env.AddNodeGroup("ng-1", 10, resource.MustParse("4"), resource.MustParse("32Gi"), nil)

		pod := env.NewTestPod("foo", resource.MustParse("1"), resource.MustParse("24Gi"))

		env.AddPod(pod).
			StepOnce().
			ExpectCommands(zalandoTestEnvironmentCommand{commandType: zalandoTestEnvironmentCommandIncreaseSize, nodeGroup: "ng-1", delta: 1}).
			AddInstance("ng-1", "i-1", false).
			AddNode("i-1", true).
			SchedulePod(pod, "i-1")

		pod2 := env.NewTestPod("bar", resource.MustParse("1"), resource.MustParse("24Gi"))
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

		pod := env.NewTestPod("foo", resource.MustParse("1"), resource.MustParse("32Gi"))

		env.AddPod(pod).
			StepOnce().
			ExpectCommands(zalandoTestEnvironmentCommand{commandType: zalandoTestEnvironmentCommandIncreaseSize, nodeGroup: "ng-1", delta: 1}).
			AddInstance("ng-1", "i-1", false).
			AddNode("i-1", true).
			SchedulePod(pod, "i-1")

		pod2 := env.NewTestPod("bar", resource.MustParse("1"), resource.MustParse("64Gi"))
		env.AddPod(pod2).
			StepFor(10 * time.Minute).
			ExpectNoCommands()

		env.AddNodeGroup("ng-2", 10, resource.MustParse("4"), resource.MustParse("64Gi"), nil).
			StepOnce().
			ExpectCommands(zalandoTestEnvironmentCommand{commandType: zalandoTestEnvironmentCommandIncreaseSize, nodeGroup: "ng-2", delta: 1})
	})
}

func TestRemoveNodeGroup(t *testing.T) {
	opts := defaultZalandoAutoscalingOptions()
	RunSimulation(t, opts, 10*time.Second, func(env *zalandoTestEnv) {
		env.AddNodeGroup("ng-1", 10, resource.MustParse("4"), resource.MustParse("32Gi"), map[string]string{labelScalePriority: "110"}).
			AddNodeGroup("ng-2", 10, resource.MustParse("4"), resource.MustParse("64Gi"), map[string]string{labelScalePriority: "100"})

		pod := env.NewTestPod("foo", resource.MustParse("1"), resource.MustParse("32Gi"))

		env.AddPod(pod).
			StepOnce().
			ExpectCommands(zalandoTestEnvironmentCommand{commandType: zalandoTestEnvironmentCommandIncreaseSize, nodeGroup: "ng-1", delta: 1}).
			AddInstance("ng-1", "i-1", false).
			AddNode("i-1", true).
			SchedulePod(pod, "i-1")

		pod2 := env.NewTestPod("bar", resource.MustParse("1"), resource.MustParse("64Gi"))
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
			env.AddPod(env.NewTestPod(fmt.Sprintf("foo-%d", i), resource.MustParse("1"), resource.MustParse("20Gi")))
		}

		env.StepFor(30*time.Second).
			AddInstance("ng-1", "i-1", false).
			AddInstance("ng-1", "i-2", false).
			AddInstance("ng-2", "i-3", false).
			AddInstance("ng-2", "i-4", false).
			AddInstance("ng-3", "i-5", false)

		for i := 5; i < 10; i++ {
			env.AddPod(env.NewTestPod(fmt.Sprintf("foo-%d", i), resource.MustParse("1"), resource.MustParse("20Gi")))
		}

		env.StepFor(2 * time.Minute)
	})
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
		ng3Pod := env.NewTestPod("pod-ng3", resource.MustParse("3"), resource.MustParse("10Gi"))
		ng3Pod.Spec.NodeSelector = map[string]string{"id": "ng-3"}

		ng2Pod := env.NewTestPod("pod-ng2", resource.MustParse("3"), resource.MustParse("10Gi"))
		ng2Pod.Spec.NodeSelector = map[string]string{"id": "ng-2"}

		env.AddPod(ng2Pod).AddPod(ng3Pod)

		env.StepOnce().
			ExpectCommands(zalandoTestEnvironmentCommand{commandType: zalandoTestEnvironmentCommandIncreaseSize, nodeGroup: "ng-3", delta: 1})

		// Run for one more minute; the node in ng-1 should be scaled down but the node in ng-2 should be kept
		env.StepFor(1*time.Minute).
			ExpectCommands(zalandoTestEnvironmentCommand{commandType: zalandoTestEnvironmentCommandDeleteNodes, nodeGroup: "ng-1", nodeNames: []string{"i-1"}}).
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

		err = deletetaint.MarkToBeDeleted(node, env.client, false)
		require.NoError(t, err)
		env.StepOnce()

		// Add a pod, this should trigger a scale-up. Upstream will erroneously consider this node as upcoming.
		env.AddPod(env.NewTestPod("foo", resource.MustParse("1"), resource.MustParse("4Gi"))).
			StepOnce().
			ExpectCommands(zalandoTestEnvironmentCommand{commandType: zalandoTestEnvironmentCommandIncreaseSize, nodeGroup: "ng-1", delta: 1})
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
		for _, fn := range []func(*corev1.Node, kube_client.Interface, bool) error{
			deletetaint.MarkToBeDeleted,
			deletetaint.MarkBeingDeleted,
		} {
			node, err := env.client.CoreV1().Nodes().Get(context.Background(), "i-1", metav1.GetOptions{})
			require.NoError(t, err)

			err = fn(node, env.client, false)
			require.NoError(t, err)
		}

		// Emulate deleting the instance on the cloud provider side (but keep it on Kubernetes side)
		env.RemoveInstance("i-1", true).
			StepOnce()

		// Add a pod, this should trigger a scale-up. Upstream will erroneously consider this node as upcoming.
		env.AddPod(env.NewTestPod("foo", resource.MustParse("1"), resource.MustParse("4Gi"))).
			StepOnce().
			ExpectCommands(zalandoTestEnvironmentCommand{commandType: zalandoTestEnvironmentCommandIncreaseSize, nodeGroup: "ng-1", delta: 1})
	})
}

func TestNodeNotReadyCustomTaint(t *testing.T) {
	opts := defaultZalandoAutoscalingOptions()

	RunSimulation(t, opts, 10*time.Second, func(env *zalandoTestEnv) {
		env.AddNodeGroup("ng-1", 10, resource.MustParse("1"), resource.MustParse("8Gi"), nil).
			AddNodeGroup("ng-2", 10, resource.MustParse("1"), resource.MustParse("8Gi"), map[string]string{labelScalePriority: "100"})

		pod := env.NewTestPod("foo", resource.MustParse("1"), resource.MustParse("4Gi"))
		env.AddPod(pod).
			StepOnce().
			ExpectCommands(zalandoTestEnvironmentCommand{commandType: zalandoTestEnvironmentCommandIncreaseSize, nodeGroup: "ng-2", delta: 1})

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
			ExpectCommands(zalandoTestEnvironmentCommand{commandType: zalandoTestEnvironmentCommandIncreaseSize, nodeGroup: "ng-1", delta: 1})

		// Add a node and schedule the pod
		env.AddInstance("ng-1", "i-2", false).
			AddNode("i-2", true).
			SchedulePod(pod, "i-2")

		// The problematic node should be decommissioned after the timeout expires
		env.StepFor(opts.ScaleDownUnreadyTime - opts.MaxNodeProvisionTime).
			ExpectNoCommands().
			StepOnce().
			ExpectCommands(zalandoTestEnvironmentCommand{commandType: zalandoTestEnvironmentCommandDeleteNodes, nodeGroup: "ng-2", nodeNames: []string{"i-1"}})
	})
}

func TestCloudProviderScalingError(t *testing.T) {
	opts := defaultZalandoAutoscalingOptions()

	RunSimulation(t, opts, 10*time.Second, func(env *zalandoTestEnv) {
		env.AddNodeGroup("ng-fallback", 10, resource.MustParse("4"), resource.MustParse("32Gi"), nil)
		env.AddNodeGroup("ng-1", 10, resource.MustParse("4"), resource.MustParse("32Gi"), map[string]string{labelScalePriority: "100"})

		p1 := env.NewTestPod("foo-1", resource.MustParse("1"), resource.MustParse("24Gi"))
		p2 := env.NewTestPod("foo-2", resource.MustParse("1"), resource.MustParse("24Gi"))
		p3 := env.NewTestPod("foo-3", resource.MustParse("1"), resource.MustParse("24Gi"))

		env.AddPod(p1).AddPod(p2).AddPod(p3).
			StepUntilNextCommand(1*time.Minute).
			ExpectCommands(zalandoTestEnvironmentCommand{commandType: zalandoTestEnvironmentCommandIncreaseSize, nodeGroup: "ng-1", delta: 3}).
			SetScaleUpError("ng-1", "we ran out of servers").
			StepOnce().StepUntilNextCommand(1*time.Minute).
			ExpectCommands(
				// Remove the two placeholder nodes, keeping one in place
				zalandoTestEnvironmentCommand{commandType: zalandoTestEnvironmentCommandDeleteNodes, nodeGroup: "ng-1", nodeNames: []string{"zalando-test:///ng-1/i-placeholder-1", "zalando-test:///ng-1/i-placeholder-2"}},

				// Fallback to the other node group
				zalandoTestEnvironmentCommand{commandType: zalandoTestEnvironmentCommandIncreaseSize, nodeGroup: "ng-fallback", delta: 3},
			).
			ExpectTargetSize("ng-1", 1).
			ExpectBackedOff("ng-1").
			AddInstance("ng-fallback", "i-1", false).AddNode("i-1", true).SchedulePod(p1, "i-1").
			AddInstance("ng-fallback", "i-2", false).AddNode("i-2", true).SchedulePod(p2, "i-2").
			AddInstance("ng-fallback", "i-3", false).AddNode("i-3", true).SchedulePod(p3, "i-3")

		// Nothing happens while the ASG is still failing
		env.StepFor(1*time.Hour).
			ExpectNoCommands().
			ExpectTargetSize("ng-1", 1).
			ExpectBackedOff("ng-1")

		// Once we get instances back, we reset the backoff and treat them normally
		env.ResetScaleUpError("ng-1").
			AddInstance("ng-1", "i-4", false).AddNode("i-4", true).
			StepFor(1*time.Minute).
			ExpectNoCommands().
			ExpectTargetSize("ng-1", 1).
			ExpectNotBackedOff("ng-1")

		// The instance should be scaled down since it's unused
		env.StepFor(10 * time.Minute).
			ExpectCommands(zalandoTestEnvironmentCommand{commandType: zalandoTestEnvironmentCommandDeleteNodes, nodeGroup: "ng-1", nodeNames: []string{"i-4"}})
	})
}

func TestEmulatedTopologySpreadConstraint(t *testing.T) {
	opts := defaultZalandoAutoscalingOptions()

	RunSimulation(t, opts, 10*time.Second, func(env *zalandoTestEnv) {
		var groups []string
		groupsPerAZ := map[string][]string{}

		currentNodeIndex := 0
		currentNodePodCount := 0

		const (
			poolCount         = 10
			replicasetCount   = 6
			scheduledReplicas = 1000
			pendingReplicas   = 150
		)

		var (
			nodeCPU    = resource.MustParse("4")
			nodeMemory = resource.MustParse("32Gi")

			replicasetPodCPU    = resource.MustParse("1m")
			replicasetPodMemory = resource.MustParse("2500Mi")

			scheduledPodsPerNode = func() int {
				count := 0
				totalCPU := resource.MustParse("0")
				totalMemory := resource.MustParse("0")

				for {
					totalCPU.Add(replicasetPodCPU)
					totalMemory.Add(replicasetPodMemory)
					if totalCPU.Cmp(nodeCPU) > 0 || totalMemory.Cmp(nodeMemory) > 0 {
						return count
					}
					count++
				}
			}()
		)

		klog.Infof("Pods per node: %d", scheduledPodsPerNode)

		for i := 0; i < poolCount; i++ {
			for _, az := range []string{"eu-central-1a", "eu-central-1b", "eu-central-1c"} {
				ngId := fmt.Sprintf("ng-%d-%s", i, az)
				env.AddNodeGroup(ngId, 10000, nodeCPU, nodeMemory, map[string]string{corev1.LabelZoneFailureDomainStable: az})
				groupsPerAZ[az] = append(groupsPerAZ[az], ngId)
				groups = append(groups, ngId)
			}
		}

		nextNode := func() {
			currentNodeIndex++
			currentNodePodCount = 0

			instanceId := fmt.Sprintf("i-%d", currentNodeIndex)

			env.AddInstance(groups[currentNodeIndex%len(groups)], instanceId, true).
				AddNode(instanceId, true)
		}

		nextNode()

		// Create a bunch of replicasets with pods
		for i := 0; i < replicasetCount; i++ {
			replicaset := env.NewTestReplicaSet(fmt.Sprintf("rs-%d", i), scheduledReplicas+pendingReplicas)
			env.AddReplicaSet(replicaset)

			for j := 0; j < scheduledReplicas; j++ {
				pod := env.NewReplicaSetPod(replicaset, replicasetPodCPU, replicasetPodMemory)
				if currentNodePodCount >= scheduledPodsPerNode {
					nextNode()
				}
				env.AddScheduledPod(WithEmulatedTopologySpreadConstraint(pod, replicaset.Name), fmt.Sprintf("i-%d", currentNodeIndex))
				currentNodePodCount++
			}

			for j := 0; j < pendingReplicas; j++ {
				pod := env.NewReplicaSetPod(replicaset, replicasetPodCPU, replicasetPodMemory)
				env.AddPod(WithEmulatedTopologySpreadConstraint(pod, replicaset.Name))
			}
		}

		nodesPerAZ := func() map[string]int {
			result := map[string]int{}
			for az, nodeGroups := range groupsPerAZ {
				for _, nodeGroup := range nodeGroups {
					result[az] += env.GetTargetSize(nodeGroup)
				}
			}
			return result
		}

		klog.Infof("Total nodes per AZ before scaling up: %v", nodesPerAZ())

		// We should have scaled up fully after just 3 iterations, and it shouldn't take ages
		env.StepOnce().StepOnce().StepOnce().ConsumeCommands()

		// I don't really want to hardcode the particular set of commands, because we don't care. Let's check that the
		// ASGs are balanced, and that we don't have too many.
		klog.Infof("Total nodes per AZ after scaling up: %v", nodesPerAZ())

		expectedNodeCount := int(math.Round(float64(replicasetCount*(scheduledReplicas+pendingReplicas)) / float64(3*scheduledPodsPerNode)))
		for az, nodeCount := range nodesPerAZ() {
			require.Contains(t, []int{expectedNodeCount, expectedNodeCount + 1}, nodeCount, "unexpected target size for zone %s: %d (expected ≈%d)", az, nodeCount, expectedNodeCount)
		}

		env.StepFor(5 * time.Minute).ExpectNoCommands()
	})
}

func TestEmulatedTopologySpreadConstraintScaleDownSameZoneBlockedNoCandidates(t *testing.T) {
	opts := defaultZalandoAutoscalingOptions()

	RunSimulation(t, opts, 10*time.Second, func(env *zalandoTestEnv) {
		rs := env.NewTestReplicaSet("foo", 2)

		rsPod1 := WithEmulatedTopologySpreadConstraint(env.NewReplicaSetPod(rs, resource.MustParse("1m"), resource.MustParse("8Gi")), rs.Name)
		rsPod2 := WithEmulatedTopologySpreadConstraint(env.NewReplicaSetPod(rs, resource.MustParse("1m"), resource.MustParse("8Gi")), rs.Name)

		env.AddNodeGroup("ng-1", 10000, resource.MustParse("4"), resource.MustParse("32Gi"), map[string]string{corev1.LabelZoneFailureDomainStable: "eu-central-1a"}).
			AddNodeGroup("ng-2", 10000, resource.MustParse("4"), resource.MustParse("32Gi"), map[string]string{corev1.LabelZoneFailureDomainStable: "eu-central-1b"}).
			AddInstance("ng-1", "i-1", true).AddNode("i-1", true).
			AddInstance("ng-2", "i-2", true).AddNode("i-2", true).
			AddReplicaSet(rs).
			AddScheduledPod(rsPod1, "i-1").
			AddScheduledPod(rsPod2, "i-2")

		// Don't scale down here because this will break the spread constraints
		env.StepFor(20 * time.Minute).
			ExpectNoCommands()
	})
}

func TestEmulatedTopologySpreadConstraintScaleDownSameZone(t *testing.T) {
	opts := defaultZalandoAutoscalingOptions()

	RunSimulation(t, opts, 10*time.Second, func(env *zalandoTestEnv) {
		rs := env.NewTestReplicaSet("foo", 2)

		rsPod1 := WithEmulatedTopologySpreadConstraint(env.NewReplicaSetPod(rs, resource.MustParse("1m"), resource.MustParse("8Gi")), rs.Name)
		rsPod2 := WithEmulatedTopologySpreadConstraint(env.NewReplicaSetPod(rs, resource.MustParse("1m"), resource.MustParse("8Gi")), rs.Name)

		env.AddNodeGroup("ng-1", 10000, resource.MustParse("4"), resource.MustParse("32Gi"), map[string]string{corev1.LabelZoneFailureDomainStable: "eu-central-1a"}).
			AddNodeGroup("ng-2", 10000, resource.MustParse("4"), resource.MustParse("32Gi"), map[string]string{corev1.LabelZoneFailureDomainStable: "eu-central-1b"}).
			AddInstance("ng-1", "i-1", true).AddNode("i-1", true).
			AddInstance("ng-2", "i-2", true).AddNode("i-2", true).
			AddInstance("ng-2", "i-3", true).AddNode("i-3", true).
			AddReplicaSet(rs).
			AddScheduledPod(rsPod1, "i-1").
			AddScheduledPod(rsPod2, "i-2").
			AddScheduledPod(env.NewTestPod("blocker", resource.MustParse("1m"), resource.MustParse("1Mi")), "i-3")

		// blocker pod can't be moved, and moving rsPod2 on the same node wouldn't break the spread constraints
		env.StepFor(20*time.Minute).
			ExpectCommands(
				zalandoTestEnvironmentCommand{commandType: zalandoTestEnvironmentCommandEvictPod, podNamespace: rsPod2.Namespace, podName: rsPod2.Name},
				zalandoTestEnvironmentCommand{commandType: zalandoTestEnvironmentCommandDeleteNodes, nodeGroup: "ng-2", nodeNames: []string{"i-2"}})
	})
}

func TestEmulatedTopologySpreadConstraintScaleUpNoFallback(t *testing.T) {
	opts := defaultZalandoAutoscalingOptions()

	RunSimulation(t, opts, 10*time.Second, func(env *zalandoTestEnv) {
		rs := env.NewTestReplicaSet("foo", 2)

		rsPod1 := WithEmulatedTopologySpreadConstraint(env.NewReplicaSetPod(rs, resource.MustParse("1m"), resource.MustParse("30Gi")), rs.Name)
		rsPod2 := WithEmulatedTopologySpreadConstraint(env.NewReplicaSetPod(rs, resource.MustParse("1m"), resource.MustParse("30Gi")), rs.Name)

		env.AddNodeGroup("ng-1", 10000, resource.MustParse("4"), resource.MustParse("32Gi"), map[string]string{corev1.LabelZoneFailureDomainStable: "eu-central-1a"}).
			AddNodeGroup("ng-2", 10000, resource.MustParse("4"), resource.MustParse("32Gi"), map[string]string{corev1.LabelZoneFailureDomainStable: "eu-central-1b"}).
			AddInstance("ng-1", "i-1", true).AddNode("i-1", true).
			AddInstance("ng-2", "i-2", true).AddNode("i-2", true).
			AddReplicaSet(rs).
			AddScheduledPod(rsPod1, "i-1").
			AddScheduledPod(env.NewTestPod("placeholder", resource.MustParse("4"), resource.MustParse("30Gi")), "i-2").
			AddPod(rsPod2)

		env.StepOnce().
			ExpectCommands(zalandoTestEnvironmentCommand{commandType: zalandoTestEnvironmentCommandIncreaseSize, nodeGroup: "ng-2", delta: 1}).
			SetScaleUpError("ng-2", "No instances are available")

		// Don't scale up other node groups
		env.StepFor(20 * time.Minute).ExpectNoCommands()
	})
}

func TestEmulatedTopologySpreadConstraintScaleUpFallbackOnTimeout(t *testing.T) {
	opts := defaultZalandoAutoscalingOptions()

	RunSimulation(t, opts, 10*time.Second, func(env *zalandoTestEnv) {
		rs := env.NewTestReplicaSet("foo", 2)

		rsPod1 := WithEmulatedTopologySpreadConstraint(env.NewReplicaSetPod(rs, resource.MustParse("1m"), resource.MustParse("30Gi")), rs.Name)
		rsPod2 := WithEmulatedTopologySpreadConstraint(env.NewReplicaSetPod(rs, resource.MustParse("1m"), resource.MustParse("30Gi")), rs.Name)

		rsPod2.Annotations = map[string]string{
			topologySpreadTimeoutAnnotation: "13m5s",
		}

		env.AddNodeGroup("ng-1", 10000, resource.MustParse("4"), resource.MustParse("32Gi"), map[string]string{corev1.LabelZoneFailureDomainStable: "eu-central-1a"}).
			AddNodeGroup("ng-2", 10000, resource.MustParse("4"), resource.MustParse("32Gi"), map[string]string{corev1.LabelZoneFailureDomainStable: "eu-central-1b"}).
			AddInstance("ng-1", "i-1", true).AddNode("i-1", true).
			AddInstance("ng-2", "i-2", true).AddNode("i-2", true).
			AddReplicaSet(rs).
			AddScheduledPod(rsPod1, "i-1").
			AddScheduledPod(env.NewTestPod("placeholder", resource.MustParse("4"), resource.MustParse("30Gi")), "i-2").
			AddPod(rsPod2)

		env.StepOnce().
			ExpectCommands(zalandoTestEnvironmentCommand{commandType: zalandoTestEnvironmentCommandIncreaseSize, nodeGroup: "ng-2", delta: 1}).
			SetScaleUpError("ng-2", "No instances are available")

		// Don't scale up other node pools until the timeout expires
		env.StepFor(13 * time.Minute).ExpectNoCommands()

		// Now we can fall back to the other zones
		env.StepOnce().ExpectCommands(zalandoTestEnvironmentCommand{commandType: zalandoTestEnvironmentCommandIncreaseSize, nodeGroup: "ng-1", delta: 1})
	})
}

func TestMaxUnschedulablePodsConsidered(t *testing.T) {
	opts := defaultZalandoAutoscalingOptions()
	opts.MaxUnschedulablePodsConsidered = 500

	RunSimulation(t, opts, 10*time.Second, func(env *zalandoTestEnv) {
		var groups []string

		currentNodeIndex := 0
		currentNodePodCount := 0

		const (
			poolCount         = 5
			replicasetCount   = 10
			scheduledReplicas = 500
			pendingReplicas   = 350
		)

		var (
			nodeCPU    = resource.MustParse("4")
			nodeMemory = resource.MustParse("32Gi")

			replicasetPodCPU    = resource.MustParse("1m")
			replicasetPodMemory = resource.MustParse("2500Mi")

			scheduledPodsPerNode = func() int {
				count := 0
				totalCPU := resource.MustParse("0")
				totalMemory := resource.MustParse("0")

				for {
					totalCPU.Add(replicasetPodCPU)
					totalMemory.Add(replicasetPodMemory)
					if totalCPU.Cmp(nodeCPU) > 0 || totalMemory.Cmp(nodeMemory) > 0 {
						return count
					}
					count++
				}
			}()
		)

		klog.Infof("Pods per node: %d", scheduledPodsPerNode)

		for i := 0; i < poolCount; i++ {
			ngId := fmt.Sprintf("ng-%d", i)
			groups = append(groups, ngId)
			env.AddNodeGroup(ngId, 10000, nodeCPU, nodeMemory, nil)
		}

		nextNode := func() {
			currentNodeIndex++
			currentNodePodCount = 0

			instanceId := fmt.Sprintf("i-%d", currentNodeIndex)

			env.AddInstance(groups[currentNodeIndex%len(groups)], instanceId, true).
				AddNode(instanceId, true)
		}

		nextNode()

		// Create a bunch of replicasets with pods
		for i := 0; i < replicasetCount; i++ {
			replicaset := env.NewTestReplicaSet(fmt.Sprintf("rs-%d", i), scheduledReplicas+pendingReplicas)
			env.AddReplicaSet(replicaset)

			for j := 0; j < scheduledReplicas; j++ {
				pod := env.NewReplicaSetPod(replicaset, replicasetPodCPU, replicasetPodMemory)
				if currentNodePodCount >= scheduledPodsPerNode {
					nextNode()
				}
				env.AddScheduledPod(pod, fmt.Sprintf("i-%d", currentNodeIndex))
				currentNodePodCount++
			}

			for j := 0; j < pendingReplicas; j++ {
				pod := env.NewReplicaSetPod(replicaset, replicasetPodCPU, replicasetPodMemory)
				env.AddPod(pod)
			}
		}

		totalNodes := func() int {
			result := 0
			for _, group := range groups {
				result += env.GetTargetSize(group)
			}
			return result
		}

		// We should only scale up for the first MaxUnschedulablePodsConsidered pods, and it shouldn't take ages
		nodesBefore := totalNodes()
		klog.Infof("Total nodes before scaling up: %d", totalNodes())
		env.StepOnce()

		nodesAfter := totalNodes()
		delta := nodesAfter - nodesBefore
		klog.Infof("Total nodes after scaling up: %d, upcoming %d", nodesAfter, delta)
		require.Contains(t, []int{opts.MaxUnschedulablePodsConsidered / scheduledPodsPerNode, (opts.MaxUnschedulablePodsConsidered / scheduledPodsPerNode) + 1}, delta)

		// Six more scale-up attempts should cover the rest of the cluster
		for i := 0; i < 6; i++ {
			env.StepOnce().ConsumeCommands()
		}

		// No further events, since all pending pods are handled
		env.StepOnce().ExpectNoCommands()
	})
}
