# Zalando Changes

The current version of the autoscaler is based on **v1.12.2** of Cluster Autoscaler with a number of
changes that we've found useful when running a significant number of autoscaling clusters on AWS.

## More robust template node generation

Upstream autoscaler always uses an existing node as a template for a node group. The cloud provider 
information (e.g. a launch configuration) is only consulted if the node group is empty. Additionally,
only the first node for each node group is selected, which might be up-to-date or not depending on the
names of the nodes. This causes a number of issues:
 
 * If a pod doesn't fit, changing the instance type will not do anything since the autoscaler will 
   continue using the capacity information from existing nodes. Manually starting a node
   will only help if the new node's name sorts before the other nodes.
 * If node labels or taints are changed, and some of the pods in the cluster use node selectors or
   affinities, it's possible that the autoscaler will not scale up because the template will pick up 
   the labels or taints from the wrong node.

The changes made in the Zalando fork make the scaling logic a lot more predictable. If the
`--scale-up-cloud-provider-template` option is set, the autoscaler will always use the template constructed
from the cloud provider configuration and will ignore existing nodes. Note that taints and labels need to be
reflected in the cloud provider configuration, for example with the `k8s.io/cluster-autoscaler/node-template/label/...`
tags on the ASG.
 
Before enabling this, please check the following:

 * Taints and labels that can affect scale up need to be reflected in the cloud provider configuration, 
   for example with the `k8s.io/cluster-autoscaler/node-template/label/...` tags on the ASG.
 * All containers in the DaemonSet pods **must** have their resource requests specified. Failing to do so
   will cause the template node size to be calculated incorrectly, and the autoscaler will scale up even
   though the pods will not fit on the newly created nodes.

## Support for AWS autoscaling groups with multiple instance types

We've added support for [autoscaling groups with multiple instance types]. This change simplifies working with
Spot pools, because instead of creating multiple pools to get decent coverage (and then still relying on
the autoscaler to [correctly detect] that a group is exhausted), cluster operators can create just one ASG
with a number of instance types and let AWS manage the instance distribution.

The template node used for simulating the scale-up will use the minimum values for all resources. For example,
an autoscaling group with `c4.xlarge` (7.5Gi memory and 4 vCPUs) and `r4.large` (15.25Gi memory and 2 vCPUs)
will be treated as if the instances had 7.5Gi memory and 2 vCPUs.

Using autoscaling groups with multiple instance types without the `--scale-up-cloud-provider-template` option
is unsupported and will likely not work correctly.

## Improved handling of template nodes

When the autoscaler generates a template node from the cloud provider configuration, it miscalculates how much
resources the node will actually have available. In normal setup this doesn't cause significant issues, other
than spurious scale-ups by 1 node followed by scale-downs, but it becomes a lot worse if the existing nodes
are ignored. For example, this might cause the autoscaler to think that a node would be able to fit a pod, scale up,
find out that the pod still doesn't fit, and then repeat until the maximum size of the node group is reached. The
unused nodes will be removed after some time, but it still causes significant churn in the cluster.

This is caused by the following issues, all of which are mitigated in the Zalando fork:

 * Daemonset pods with limits but no requirements are treated as if they had zero size (instead of requests being
   the same as limits).
 * The amount of available memory is usually smaller than what AWS API provides. Additionally, resources reserved 
   by the system (via the `--system-reserved` and `kube-reserved`) are not accounted for. In the Zalando version, 
   the autoscaler collects information about node capacity and allocatable from the existing nodes, and tries to
   estimate the amount reserved resources for instance types that it hasn't previously seen.

Additionally, the upstream version of the autoscaler always assumes that AWS nodes would have a `kube-proxy` pod
consuming a static amount of CPU but no memory, which will slightly reduce the CPU available for the user pods.
In the forked version, we've added a workaround that tries to undo this. 

## Expander that prefers Spot node groups

The `prefer-spot` expander will always scale up a Spot node group if it's available. This requires the template
nodes returned from this ASG to have the `aws.amazon.com/spot` label set to `true` for Spot ASGs and `false`
(or unset) for On Demand ones. It falls back to the `random` expander afterwards.

[autoscaling groups with multiple instance types]: https://aws.amazon.com/blogs/aws/new-ec2-auto-scaling-groups-with-multiple-instance-types-purchase-options/
[correctly detect]: https://github.com/kubernetes/autoscaler/issues/1133
