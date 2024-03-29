# Proofs of Concept for Trace Sampling in OpenTelemetry

This repository contains proofs of concept related to span sampling and ideas presented in the paper [Estimation from Partially Sampled Distributed Traces](https://arxiv.org/pdf/2107.07703v1.pdf).


* [AbstractConsistentSampler.java](https://github.com/dynatrace-research/opentelemetry-sampling-poc/blob/master/poc/src/main/java/com/dynatrace/research/otelsampling/sampling/AbstractConsistentSampler.java) is an abstract sampler implementation that ensures consistent sampling with rates that are restricted to powers of two. The sampler also puts extra information to the trace state that allows linking to the closest sampled ancestor span as described in the paper in Section 2.10. There are two implementations:
  * [ConsistentParentRateSampler.java](https://github.com/dynatrace-research/opentelemetry-sampling-poc/blob/master/poc/src/main/java/com/dynatrace/research/otelsampling/sampling/ConsistentParentRateSampler.java): This sampler uses the parent sample rate.
  * [ConsistentFixedRateSampler.java](https://github.com/dynatrace-research/opentelemetry-sampling-poc/blob/master/poc/src/main/java/com/dynatrace/research/otelsampling/sampling/ConsistentFixedRateSampler.java): This sampler uses a fixed sampling rate. If the sampling rate is not exactly a power of 1/2 it randomly switches between the neighboring power of 1/2 sampling rates such that the effective sampling rate matches the configured one as proposed in the paper.
* [EstimationUtil.java](https://github.com/dynatrace-research/opentelemetry-sampling-poc/blob/master/poc/src/main/java/com/dynatrace/research/otelsampling/estimation/EstimationUtil.java) demonstrates the estimation algorithm for partially sampled traces as described in the paper.
* [ReservoirSampler.java](https://github.com/dynatrace-research/opentelemetry-sampling-poc/blob/master/poc/src/main/java/com/dynatrace/research/otelsampling/sampling/ReservoirSampler.java) demonstrates consistent sampling of spans with a fixed size reservoir (buffer) where the individual span sampling rates are restricted to a predefined discrete set.


In the meantime, these concepts are already integrated in 
https://github.com/dynatrace-oss-contrib/opentelemetry-java-contrib.
