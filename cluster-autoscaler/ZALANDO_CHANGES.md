# Zalando Changes

The current version of the autoscaler is based on **v1.18.2** of Cluster Autoscaler with a number of
changes that we've found useful when running a significant number of autoscaling clusters on AWS.

## More robust template node generation

Upstream autoscaler always uses an existing node as a template for a node group. The cloud provider 
information (e.g. a launch configuration) is only consulted if the node group is empty. Additionally,
only the first node for each node group is selected, which might be up-to-date or not depending on the
names of the nodes. This causes a number of issues:
 
 * If a pod doesn't fit, changing the instance type will not do anything since the autoscaler will 
   continue using the capacity information from existing nodes. Manually starting a node
   will only help if the new node's name sorts before the other nodes.
 * If node labels or taints are changed, and some pods in the cluster use node selectors or affinities, it's
   possible that the autoscaler will not scale up because the template will pick up the labels or taints from
   the wrong node.

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
   
Using this option will also disable the node info cache added in v1.13. The cache has some issues, such as
not handling daemonset changes correctly, and the template nodes represent much more precise information 
about what upcoming nodes would look like.

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

## More reliable backoff logic

We often run into issues with instance availability for particular node group in our clusters. Because the AWS cloud
provider doesn't support properly reporting scale-up errors, we have to rely on the scale-up timeout logic to recognise
these issues and trigger a fallback. When the upstream version detects a scale-up timeout, it marks the node group as
failed (with an exponential backoff starting at 5 minutes and a maximum of 30 minutes) and resets the scale-up
on the cloud provider side.

This logic, however, is rather problematic in clusters where a significant number of node groups could be affected at
the same time. Let's say we have 4 node groups, `n1` to `n4`, a scale-up timeout of 7 minutes (which might be hard to
reduce further), and node groups `n1` to `n3` have run out of instances. The upstream version of the autoscaler will
do something like this:
 * Scale-up node group `n1`.
 * Wait for the timeout to trigger (~7 minutes). 
 * Place `n1` in backoff (5 minutes), scale-up `n2`.
 * Wait for the timeout to trigger. During this time the backoff on `n1` will expire, making it healthy again.
 * Place `n2` in backoff (5 minutes), scale-up `n1` since it's now healthy again.
 * Wait for the timeout to trigger (10 minutes).
 * …
 * Scale-up `n4` once the backoff time for ther other groups is big enough. This can take hours in a cluster that
   has a lot of node groups.

Starting the backoff at a large value (~1 hour or more) mitigates this, but it can negatively impact clusters that have
small amounts of node groups. A transient scale-up error that quickly disappears on the cloud provider side can prevent
scale-up for an hour or more.

In our fork, when a scale-up timeout occurs, the autoscaler places the node group in the backoff state (which doesn't
expire) and resets the size on the cloud provider side to the current capacity plus one. The node group is not
considered in future scale-up attempts, and the backoff is cleared only when the requested instance joins the cluster.
The autoscaler also exposes the current state of the node groups in its metrics, allowing the operators to monitor the
backoffs.

## Improved handling of template nodes

When the autoscaler generates a template node from the cloud provider configuration, it miscalculates how many
resources the node will actually have available. Normally this doesn't cause significant issues, other than spurious
scale-ups by 1 node followed by scale-downs, but it becomes worse if the existing nodes are ignored. For example,
this might cause the autoscaler to think that a node would be able to fit a pod, scale up, find out that the pod still
doesn't fit, and then repeat until the maximum size of the node group is reached. The unused nodes will be removed after
some time, but it still causes significant churn in the cluster.

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

## Allow scale-down to make progress in the presence of PDBs

Scale-down of nodes that run user pods is performed in multiple steps. The node is first tainted with a `NoSchedule`
taint, then the user pods are evicted, and afterwards it's terminated on the cloud provider side. In the upstream
version, the draining process has a hardcoded timeout of 2 minutes. If the timeout expires, the scale-down attempt is
considered unsuccessful, and the autoscaler reverts the changes made to the node.

This can result in completely breaking scale-down if there are any applications that take long enough to start and at
the same time use a PDB that heavily restricts the number of unavailable replicas. In this situation, the autoscaler
would keep terminating the user pods without making any actual progress when scaling down. For example, let's say we
have three nodes (`n1`, `n2` and `n3`), each of them runs 10 replicas of the `example` application that takes 30 seconds
to start and become ready, and there's a PDB that only allows one replica to be unavailable. If all three nodes can be
scaled down, the autoscaler would end up doing something like this:
 * Select one of the nodes for scale-down (let's say `n1`).
 * Add the decommissioning taint, start evicting pods one at a time.
 * Successfully evict the first pod, then fail on the remaining ones for the next 30 seconds.
 * Successfully evict the second and the third pods.
 * Time out, untaint the node, mark the scale-down as failed.
 * Do nothing until the failed scale-down timeout expires.
 * Repeat the same process with any of the three nodes again. If it doesn't choose node `n1` again, evicted
   pods will be moved back.

In our version, the scale-down timeout is configurable, and we run with a much higher value by default. This doesn't
solve the issue fully, but seems to be enough for the workloads we run. We've also added metrics to track how long it
takes to scale-down, because the upstream metrics were not updated after the logic was made asynchronous and as a result
don't work correctly.

## Tooling to simulate the autoscaler behaviour

The existing logic of the autoscaler is spread around many internal subsystems, and we've found it very hard to predict
how a particular sequence of events will affect the scaling. The existing test coverage is not very helpful because
most of the tests target individual units, use test-only dependencies that don't behave exactly the same as the real
ones, and the amount of boilerplate needed to set up the initial state and modify it afterwards is enormous. We've
written a mock cloud provider and a set of helpers that allows us to write very high-level simulations of how the
autoscaler would behave in particular circumstances. The examples are available in [zalando_simulation_test.go].

This tooling does not cover everything, because at some point a lot of the autoscaler logic was rewritten to spawn
goroutines which makes it very hard to test, but even the existing coverage has allowed us to quickly identify the
issues with the scale-up logic and ensure that further changes would not break our mitigations.

## Fix a bug in the NodeInfo.Clone() function that corrupts internal state

The autoscaler relies on `NodeInfo.Clone()` in a couple of places. That function, unfortunately, is bugged and doesn't
correctly clone a lot of the internal state. The most glaring example is the underlying Node object, where it just
copies the pointer instead of doing a DeepCopy. This obviously causes issues for code that patches the copied node
afterwards, like the function that creates template nodes, which completely breaks scale-up logic in the presence of
unregistered nodes.

## Ensure that scale-up works correctly during scale-down

The autoscaler erroneously considers the nodes that are currently being scaled down as upcoming ones. This means that if
the pending pods can be scheduled on the node that's currently being deleted, it would not trigger a scale-up. With the
increased duration of the scale-down attempts (see the above change), this effectively breaks scale-up for hours at a
time. This was fixed in our fork.

## Priority-based expander based on node labels

The `highest-priority` expander selects an expansion option based on the priorities assigned to a node group. Unlike
the `priority` expander, it reads the scaling priority from the label of a template node, which makes it slightly easier
to configure. For groups with the same priority value, it falls back to the `random` expander.

## Job pods are considered unmovable

The upstream version treats Job pods as replicated ones, which means can be terminated and moved to other nodes. For
long-running jobs, this can result in major delays in completion (if they would complete at all) and lots of wasted
duplicate work. As a result, our fork treats Job pods as unmovable.

## An attempt at improving the performance of the TopologySpreadConstraint predicate

One of the main goals of our update from 1.12 to 1.18 was the support for the `TopologySpreadConstraint` predicate.
After the initial tests showed major performance issues, we've tried to add some band-aids to the scale-up logic to
optimise it, but in the end we've had to roll back and disable the predicate completely. Unfortunately, the design of
both the scheduler framework and the code that uses it is deeply flawed, with some functions scaling quadratically (or
even higher) with the number of pods, making it completely unusable in medium-sized clusters.   

[autoscaling groups with multiple instance types]: https://aws.amazon.com/blogs/aws/new-ec2-auto-scaling-groups-with-multiple-instance-types-purchase-options/
[correctly detect]: https://github.com/kubernetes/autoscaler/issues/1133
[zalando_simulation_test.go]: https://github.com/zalando-incubator/autoscaler/blob/zalando-cluster-autoscaler/cluster-autoscaler/core/zalando_simulation_test.go
