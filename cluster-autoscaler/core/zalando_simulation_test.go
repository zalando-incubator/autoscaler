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
	"flag"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"
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
	opts.BackoffNoFullScaleDown = true
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
	opts.BackoffNoFullScaleDown = true
	RunSimulation(t, opts, 10*time.Second, func(env *zalandoTestEnv) {
		env.AddNodeGroup("ng-fallback", 10, resource.MustParse("4"), resource.MustParse("32Gi"), nil)
		env.AddNodeGroup("ng-1", 10, resource.MustParse("4"), resource.MustParse("32Gi"), map[string]string{labelScalePriority: "100"})

		env.SetTargetSize("ng-1", 1)

		env.AddPod(NewTestPod("foo", resource.MustParse("1"), resource.MustParse("24Gi"))).
			StepUntilCommand(20*time.Minute, zalandoCloudProviderCommand{commandType: zalandoCloudProviderCommandIncreaseSize, nodeGroup: "ng-fallback", delta: 1})
	})
}
