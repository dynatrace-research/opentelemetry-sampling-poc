/*
 * Copyright (c) 2012-2021 Dynatrace LLC. All rights reserved.
 *
 * This software and associated documentation files (the "Software")
 * are being made available by Dynatrace LLC for purposes of
 * illustrating the implementation of certain algorithms which have
 * been published by Dynatrace LLC. Permission is hereby granted,
 * free of charge, to any person obtaining a copy of the Software,
 * to view and use the Software for internal, non-productive,
 * non-commercial purposes only – the Software may not be used to
 * process live data or distributed, sublicensed, modified and/or
 * sold either alone or as part of or in combination with any other
 * software.
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */
package com.dynatrace.research.otelsampling.sampling;

import static java.util.Objects.requireNonNull;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.trace.data.LinkData;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.sdk.trace.samplers.SamplingDecision;
import io.opentelemetry.sdk.trace.samplers.SamplingResult;
import java.util.*;
import java.util.function.BooleanSupplier;

public abstract class AbstractConsistentSampler implements Sampler {

  public static final int SAMPLING_UNKNOWN_RATE_EXPONENT = 0;

  public static final String SAMPLING_GEOMETRIC_RANDOM_VALUE_KEY =
      "sampling-geometric-random-value";
  public static final String SAMPLING_RATE_EXPONENT_KEY = "sampling-rate-exponent";

  protected final BooleanSupplier threadSafeRandomGenerator;

  public AbstractConsistentSampler(BooleanSupplier threadSafeRandomGenerator) {
    this.threadSafeRandomGenerator = requireNonNull(threadSafeRandomGenerator);
  }

  // returns a random value from a geometric distribution with a success probability of 0.5 and
  // minimum value 1 that is
  // clipped at 62
  private int generateGeometricRandomValue() {
    int count = 1;
    while (count < 62 && threadSafeRandomGenerator.getAsBoolean()) {
      count += 1;
    }
    return count;
  }

  protected int getGeometricRandomValueFromParentContextOrGenerate(Context parentContext) {
    Span parentSpan = Span.fromContext(parentContext);
    String geometricRandomValueAsString =
        parentSpan.getSpanContext().getTraceState().get(SAMPLING_GEOMETRIC_RANDOM_VALUE_KEY);
    if (geometricRandomValueAsString != null) {
      int geometricRandomValue =
          Integer.parseInt(geometricRandomValueAsString); // TODO exception handling
      if (geometricRandomValue >= 1 && geometricRandomValue <= 63) {
        return geometricRandomValue;
      }
    }
    return generateGeometricRandomValue();
  }

  protected int getParentSamplingRateExponentFromParentContext(Context parentContext) {
    Span parentSpan = Span.fromContext(parentContext);
    String pow2ParentSaplingRateAsString =
        parentSpan.getSpanContext().getTraceState().get(SAMPLING_RATE_EXPONENT_KEY);
    if (pow2ParentSaplingRateAsString != null) {
      int parentSamplingRateExponent =
          Integer.parseInt(pow2ParentSaplingRateAsString); // TODO exception handling
      if (parentSamplingRateExponent >= 0 && parentSamplingRateExponent <= 63) {
        return parentSamplingRateExponent;
      }
    }
    return SAMPLING_UNKNOWN_RATE_EXPONENT;
  }

  @Override
  public final SamplingResult shouldSample(
      Context parentContext,
      String traceId,
      String name,
      SpanKind spanKind,
      Attributes attributes,
      List<LinkData> parentLinks) {

    final int geometricRandomValue =
        getGeometricRandomValueFromParentContextOrGenerate(parentContext);

    final int samplingRateExponent =
        getSamplingRateExponent(parentContext, traceId, name, spanKind, attributes, parentLinks);

    boolean samplingDecision = geometricRandomValue >= samplingRateExponent;

    if (samplingDecision) {
      return new SamplingResult() {

        @Override
        public SamplingDecision getDecision() {
          return SamplingDecision.RECORD_AND_SAMPLE;
        }

        @Override
        public Attributes getAttributes() {
          return Attributes.empty();
        }

        @Override
        public TraceState getUpdatedTraceState(TraceState parentTraceState) {
          return parentTraceState.toBuilder()
              .put(SAMPLING_GEOMETRIC_RANDOM_VALUE_KEY, Integer.toString(geometricRandomValue))
              .put(SAMPLING_RATE_EXPONENT_KEY, Integer.toString(samplingRateExponent))
              .build();
        }
      };
    } else {
      return new SamplingResult() {

        @Override
        public SamplingDecision getDecision() {
          return SamplingDecision.DROP;
        }

        @Override
        public Attributes getAttributes() {
          return Attributes.empty();
        }

        @Override
        public TraceState getUpdatedTraceState(TraceState parentTraceState) {
          return parentTraceState.toBuilder()
              .put(SAMPLING_GEOMETRIC_RANDOM_VALUE_KEY, Integer.toString(geometricRandomValue))
              .put(SAMPLING_RATE_EXPONENT_KEY, Integer.toString(samplingRateExponent))
              .build();
        }
      };
    }
  }

  // must return a value in the range [1, 63]
  //  1 means sampling rate = 1
  //  2 means sampling rate = 1/2
  //  3 means sampling rate = 1/4
  //  ...
  // 61 means sampling rate = 1/2^60
  // 62 means sampling rate = 1/2^61
  // 63 means sampling rate = 0
  protected abstract int getSamplingRateExponent(
      Context parentContext,
      String traceId,
      String name,
      SpanKind spanKind,
      Attributes attributes,
      List<LinkData> parentLinks);

  protected static double getSamplingRate(int samplingRateExponent) {
    if (samplingRateExponent < 1 || samplingRateExponent > 63) {
      throw new IllegalArgumentException("Sampling rate exponent must be in the range [1, 63]!");
    }
    if (samplingRateExponent == 63) {
      return 0.;
    } else {
      return 1. / (1L << (samplingRateExponent - 1));
    }
  }
}