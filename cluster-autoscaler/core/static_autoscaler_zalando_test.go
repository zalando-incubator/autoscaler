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

func TestExampleSimulationTest(t *testing.T) {
	RunSimulation(t, defaultZalandoAutoscalingOptions(), 10*time.Second, func(env *zalandoTestEnv) {
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

		env.AddNode("i-1", true)
		klog.Info("added node")
		env.StepFor(1 * time.Minute).ExpectNoCommands()

		env.SchedulePod(pod.Name, "i-1")
		klog.Info("scheduled pod")
		env.StepFor(15 * time.Minute).ExpectNoCommands()

		env.RemovePod(pod.Name)
		klog.Info("removed pod")

		env.StepFor(15 * time.Minute).ExpectCommands(zalandoCloudProviderCommand{
			commandType: zalandoCloudProviderCommandDeleteNodes,
			nodeGroup:   "ng-1",
			nodeNames:   []string{"i-1"},
		})

		env.RemoveNode("i-1")

		for _, group := range env.cloudProvider.nodeGroups {
			klog.Infof("%s: instances: %s", group.id, group.instances)
			klog.Infof("%s: ts: %d", group.id,group.targetSize)
		}

		klog.Info("removed node")
		env.StepFor(15 * time.Minute).ExpectNoCommands()
	})
}
