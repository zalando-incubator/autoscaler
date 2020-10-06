package core

import (
	"flag"
	"os"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/klog"
)

func TestMain(m *testing.M) {
	klog.InitFlags(flag.CommandLine)
	err := flag.Set("v", "3")
	if err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}

func TestBasicScaleUpTest(t *testing.T) {
	env := newZalandoTestEnv(t, defaultZalandoAutoscalingOptions(), 10*time.Second)
	env.AddNodeGroup("ng-1", 10, resource.MustParse("4"), resource.MustParse("32Gi"), nil)
	env.StepOnce().ExpectNoCommands()

	rs := NewTestReplicaSet("foo", 1)
	env.AddReplicaSet(rs)

	pod := NewReplicaSetPod(rs, resource.MustParse("1"), resource.MustParse("8Gi"))
	env.AddPod(pod)
	env.StepOnce().ExpectCommands(zalandoCloudProviderCommand{
		commandType: zalandoCloudProviderCommandIncreaseSize,
		nodeGroup:   "ng-1",
		delta:       1,
	})

	klog.Info("scaled up")
	env.StepFor(1 * time.Minute).ExpectNoCommands()

	env.AddInstance("ng-1", "i-1")
	klog.Info("added instance")
	env.StepFor(1 * time.Minute).ExpectNoCommands()

	env.AddNode("i-1")
	klog.Info("added node")
	env.StepFor(1 * time.Minute).ExpectNoCommands()

	env.SchedulePod(pod.Name, "i-1")
	klog.Info("scheduled pod")
	env.StepFor(25 * time.Minute).ExpectNoCommands()
}
