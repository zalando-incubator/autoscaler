package core

import (
	"flag"
	"os"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/klog"
)

const (
	labelScalePriority = "zalando.org/scaling-priority"
)

func TestMain(m *testing.M) {
	klog.InitFlags(flag.CommandLine)
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
		env.AddNodeGroup("ng-2", 10, resource.MustParse("4"), resource.MustParse("32Gi"), map[string]string{labelScalePriority: "100"})
		env.AddNodeGroup("ng-3", 10, resource.MustParse("4"), resource.MustParse("32Gi"), map[string]string{labelScalePriority: "100"})

		env.StepFor(30 * time.Second).ExpectNoCommands()

		pod := NewTestPod("foo", resource.MustParse("1"), resource.MustParse("8Gi"))
		env.AddPod(pod)

		env.StepUntilCommand(24*time.Hour, zalandoCloudProviderCommand{
			commandType: zalandoCloudProviderCommandIncreaseSize,
			nodeGroup:   "ng-fallback",
			delta:       1,
		})
		env.LogStatus()
	})
}

func TestZalandoScalingTest(t *testing.T) {
	opts := defaultZalandoAutoscalingOptions()
	opts.BackoffNoFullScaleDown = true
	RunSimulation(t, opts, 10*time.Second, func(env *zalandoTestEnv) {
		env.AddNodeGroup("ng-fallback", 10, resource.MustParse("4"), resource.MustParse("32Gi"), nil)
		env.AddNodeGroup("ng-1", 10, resource.MustParse("4"), resource.MustParse("32Gi"), map[string]string{labelScalePriority: "100"})
		env.AddNodeGroup("ng-2", 10, resource.MustParse("4"), resource.MustParse("32Gi"), map[string]string{labelScalePriority: "100"})
		env.AddNodeGroup("ng-3", 10, resource.MustParse("4"), resource.MustParse("32Gi"), map[string]string{labelScalePriority: "100"})

		env.StepFor(30 * time.Second).ExpectNoCommands()

		pod := NewTestPod("foo", resource.MustParse("1"), resource.MustParse("8Gi"))
		env.AddPod(pod)

		env.StepUntilCommand(24*time.Hour, zalandoCloudProviderCommand{
			commandType: zalandoCloudProviderCommandIncreaseSize,
			nodeGroup:   "ng-fallback",
			delta:       1,
		})
		env.LogStatus()
	})
}
