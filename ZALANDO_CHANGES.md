# Zalando Changes

The current forked version of the Vertical Pod Autoscaler (VPA) is based on
upstream **v0.11.0** with a number of changes that we've found useful when
running a significant number of vertical pod autoscaled workloads in
Kubernetes.

Our goal is to eventually upstream most or all of these changes for the good of
the Kubernetes community.

## Improvements around OOMKill handling

While running the VPA in our clusters we observed that most problems occurred
when an application would spike in memory usage and thus get OOM killed. In
these cases the VPA was not reacting fast enough and thus we have made several
changes around the OOMKill handling to improve this situation:

* Always delete pods on quick OOMs ([#21](https://github.com/zalando-incubator/autoscaler/pull/21))
* Force record a sample in case of OOMKill ([#41](https://github.com/zalando-incubator/autoscaler/pull/41))

## Variuos small improvements

* Fix logging for memory saver mode ([#18](https://github.com/zalando-incubator/autoscaler/pull/18))
