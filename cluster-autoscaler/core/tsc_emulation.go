/*
Copyright 2021 The Kubernetes Authors.

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
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/autoscaler/cluster-autoscaler/config"
	"k8s.io/autoscaler/cluster-autoscaler/utils/kubernetes"
	klog "k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/util/slice"
)

const (
	topologySpreadTimeoutAnnotation = "zalando.org/topology-spread-timeout"
)

type azDistribution map[string]int

func (d azDistribution) addPod(zoneName string) {
	d[zoneName] += 1
}

func (d azDistribution) smallestZone(availableZones []string) string {
	currentCandidate := ""
	currentCount := 0
	for _, zone := range availableZones {
		zoneCount := d[zone]
		if currentCandidate == "" || zoneCount < currentCount {
			currentCandidate = zone
			currentCount = zoneCount
		}
	}
	return currentCandidate
}

type azDistributions map[string]azDistribution

func (d azDistributions) distributionFor(distributionId string) azDistribution {
	result := d[distributionId]
	if result == nil {
		result = azDistribution{}
		d[distributionId] = result
	}
	return result
}

type topologySpreadEmulationScheduledPodLister struct {
	base            kubernetes.PodLister
	constraintLabel string
	nodeZoneMapping map[string]string
}

func topologySpreadEmulationWrapScheduledPodLister(base kubernetes.PodLister, options config.AutoscalingOptions, nodeZoneMapping map[string]string) kubernetes.PodLister {
	if options.EmulatedTopologySpreadConstraintLabel == "" {
		return base
	}
	return &topologySpreadEmulationScheduledPodLister{
		base:            base,
		constraintLabel: options.EmulatedTopologySpreadConstraintLabel,
		nodeZoneMapping: nodeZoneMapping,
	}
}

func (l *topologySpreadEmulationScheduledPodLister) List() ([]*corev1.Pod, error) {
	original, err := l.base.List()
	if err != nil {
		return nil, err
	}

	var result []*corev1.Pod
	for _, pod := range original {
		if hasEmulatedTopologySpreadConstraints(pod, l.constraintLabel) {
			pod = pod.DeepCopy()
			pod.Spec.TopologySpreadConstraints = nil

			// Pin already scheduled pods to their current zones, to avoid potential issues with downscaling
			if nodeZone, ok := l.nodeZoneMapping[pod.Spec.NodeName]; ok && nodeZone != "" {
				if pod.Spec.NodeSelector == nil {
					pod.Spec.NodeSelector = map[string]string{}
				}
				pod.Spec.NodeSelector[corev1.LabelZoneFailureDomainStable] = nodeZone
			}
		}
		result = append(result, pod)
	}
	return result, nil
}

type topologySpreadEmulationUnschedulablePodLister struct {
	base                 kubernetes.PodLister
	constraintLabel      string
	availableZones       []string
	currentDistributions azDistributions
	currentTime          time.Time
}

func topologySpreadEmulationWrapUnschedulablePodLister(base kubernetes.PodLister, options config.AutoscalingOptions, availableZones []string, currentDistributions azDistributions, currentTime time.Time) kubernetes.PodLister {
	if options.EmulatedTopologySpreadConstraintLabel == "" {
		return base
	}
	return &topologySpreadEmulationUnschedulablePodLister{
		base:                 base,
		constraintLabel:      options.EmulatedTopologySpreadConstraintLabel,
		availableZones:       availableZones,
		currentDistributions: currentDistributions,
		currentTime:          currentTime,
	}
}

func (l *topologySpreadEmulationUnschedulablePodLister) List() ([]*corev1.Pod, error) {
	original, err := l.base.List()
	if err != nil {
		return nil, err
	}

	var result []*corev1.Pod
	for _, pod := range original {
		if distributionId, ok := getDistributionId(pod, l.constraintLabel); ok && hasEmulatedTopologySpreadConstraints(pod, l.constraintLabel) {
			pod = pod.DeepCopy()

			pod.Spec.TopologySpreadConstraints = nil
			if l.timeoutExpired(pod) {
				klog.V(4).Infof("Timeout expired for pod %s/%s, allowing it to run in any zone", pod.Namespace, pod.Name)
			} else {
				currentDistribution := l.currentDistributions.distributionFor(distributionId)
				nextZone := currentDistribution.smallestZone(l.availableZones)
				if pod.Spec.NodeSelector == nil {
					pod.Spec.NodeSelector = map[string]string{}
				}
				pod.Spec.NodeSelector[corev1.LabelZoneFailureDomainStable] = nextZone
				currentDistribution.addPod(nextZone)
				klog.V(4).Infof("Assigning pod %s/%s to zone %s, changing the distribution to %v", pod.Namespace, pod.Name, nextZone, currentDistribution)
			}
		}
		result = append(result, pod)
	}
	return result, nil
}

func (l *topologySpreadEmulationUnschedulablePodLister) timeoutExpired(pod *corev1.Pod) bool {
	if timeoutStr, ok := pod.Annotations[topologySpreadTimeoutAnnotation]; ok {
		if timeout, err := time.ParseDuration(timeoutStr); err == nil {
			return pod.CreationTimestamp.Time.Add(timeout).Before(l.currentTime)
		}
	}
	return false
}

func collectNodeZoneMapping(nodes []*corev1.Node) map[string]string {
	result := map[string]string{}
	for _, node := range nodes {
		if zone, ok := node.Labels[corev1.LabelZoneFailureDomainStable]; ok {
			result[node.Name] = zone
		}
	}
	return result
}

func collectAvailableZones(nodes []*corev1.Node) []string {
	zones := sets.NewString()
	for _, node := range nodes {
		if zone, ok := node.Labels[corev1.LabelZoneFailureDomainStable]; ok {
			zones.Insert(zone)
		}
	}
	return slice.SortStrings(zones.List())
}

func getDistributionId(pod *corev1.Pod, labelName string) (string, bool) {
	if hash, ok := pod.Labels[labelName]; ok && hash != "" {
		return pod.Namespace + "/" + hash, true
	}
	return "", false
}

func (a *StaticAutoscaler) collectAZDistributions(zoneMapping map[string]string, pods []*corev1.Pod) azDistributions {
	if a.EmulatedTopologySpreadConstraintLabel == "" {
		return azDistributions{}
	}

	distributionInfo := azDistributions{}
	for _, pod := range pods {
		// No node assigned, or pod is not active anymore
		if pod.Spec.NodeName == "" || pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		// Can't determine the zone, let's ignore for now
		zoneName, ok := zoneMapping[pod.Spec.NodeName]
		if !ok {
			continue
		}

		if distributionId, ok := getDistributionId(pod, a.EmulatedTopologySpreadConstraintLabel); ok {
			distributionInfo.distributionFor(distributionId).addPod(zoneName)
		}
	}
	klog.V(4).Infof("Current TSC distributions: %v", distributionInfo)
	return distributionInfo
}

func hasEmulatedTopologySpreadConstraints(pod *corev1.Pod, constraintLabel string) bool {
	if len(pod.Spec.TopologySpreadConstraints) != 1 {
		return false
	}
	constraint := pod.Spec.TopologySpreadConstraints[0]

	// Check if the constraints match the automatically injected TSCs. We won't bother emulating anything else, instead
	// we'll reject them at admission time.
	return constraint.TopologyKey == corev1.LabelZoneFailureDomainStable &&
		constraint.MaxSkew == 1 &&
		constraint.WhenUnsatisfiable == corev1.DoNotSchedule &&
		len(constraint.LabelSelector.MatchExpressions) == 0 &&
		len(constraint.LabelSelector.MatchLabels) == 1 &&
		constraint.LabelSelector.MatchLabels[constraintLabel] != "" &&
		constraint.LabelSelector.MatchLabels[constraintLabel] == pod.Labels[constraintLabel]
}
