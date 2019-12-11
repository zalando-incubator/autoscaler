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

// Package oomkill - code to detect oomkills in containers
package oomkill

import (
	"time"

	apiv1 "k8s.io/api/core/v1"
)

// HasQuickOomKill checks if one of the containers of the pod has been OOMKilled in the duration specified
func HasQuickOomKill(lifetime time.Duration, pod *apiv1.Pod) bool {
	for _, containerStatus := range pod.Status.ContainerStatuses {
		terminationState := containerStatus.LastTerminationState
		if terminationState.Terminated != nil &&
			terminationState.Terminated.Reason == "OOMKilled" &&
			terminationState.Terminated.FinishedAt.Time.Sub(terminationState.Terminated.StartedAt.Time) < lifetime {
			return true
		}
	}
	return false
}
