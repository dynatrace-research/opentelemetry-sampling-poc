# Proofs of Concept for Trace Sampling in OpenTelemetry

This repository contains proofs of concept related to span sampling and ideas presented in the paper [Estimation from Partially Sampled Distributed Traces](https://arxiv.org/pdf/2107.07703v1.pdf).

* [EstimationUtil.java](https://github.com/dynatrace-research/opentelemetry-sampling-poc/blob/master/proof-of-concept/src/main/java/com/dynatrace/research/otelsampling/estimation/EstimationUtil.java) demonstrates the estimation algorithm for partially sampled traces as described in the paper.
* [AdvancedTraceIdRatioBasedSampler.java](https://github.com/dynatrace-research/opentelemetry-sampling-poc/blob/master/proof-of-concept/src/main/java/com/dynatrace/research/otelsampling/sampling/AdvancedTraceIdRatioBasedSampler.java) is a sampler implementation that collects the sampling probability and optionally puts extra information to the trace state that allows linking to the closest sampled ancestor span as described in the paper in Section 2.10.
* [ReservoirSampler.java](https://github.com/dynatrace-research/opentelemetry-sampling-poc/blob/master/proof-of-concept/src/main/java/com/dynatrace/research/otelsampling/sampling/ReservoirSampler.java) demonstrates consistent sampling of spans with a fixed size reservoir (buffer) where the individual span sampling rates are restricted to a predefined discrete set.
