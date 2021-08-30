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

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.api.trace.TraceStateBuilder;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.trace.data.LinkData;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.sdk.trace.samplers.SamplingDecision;
import io.opentelemetry.sdk.trace.samplers.SamplingResult;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public abstract class AbstractConsistentSampler implements Sampler {

  public static final int SAMPLING_UNKNOWN_RATE_EXPONENT = 0;

  public static final String SAMPLING_GEOMETRIC_RANDOM_VALUE_KEY =
      "sampling-geometric-random-value";
  public static final String SAMPLING_RATE_EXPONENT_KEY = "sampling-rate-exponent";
  public static final String NUMBER_DROPPED_ANCESTORS_KEY = "number-dropped-ancestors";
  public static final String SAMPLED_ANCESTOR_SPAN_ID_KEY = "sampled-ancestor-span-id";

  protected boolean generateRandomBit() {
    return ThreadLocalRandom.current().nextBoolean();
  }

  protected RecordingMode getRecordingMode() {
    return RecordingMode.ANCESTOR_LINK_AND_DISTANCE;
  }

  // returns a random value from a geometric distribution with a success probability of 0.5 and
  // minimum value 1 that is clipped at 62
  private int generateGeometricRandomValue() {
    int count = 1;
    while (count < 62 && generateRandomBit()) {
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

    RecordingMode recordingMode = getRecordingMode();

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
          TraceStateBuilder builder = parentTraceState.toBuilder();
          builder.put(SAMPLING_GEOMETRIC_RANDOM_VALUE_KEY, Integer.toString(geometricRandomValue));
          builder.put(SAMPLING_RATE_EXPONENT_KEY, Integer.toString(samplingRateExponent));
          builder.remove(NUMBER_DROPPED_ANCESTORS_KEY);
          builder.remove(SAMPLED_ANCESTOR_SPAN_ID_KEY);
          return builder.build();
        }
      };
    } else {

      String parentSpanId = Span.fromContext(parentContext).getSpanContext().getSpanId();

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

          TraceStateBuilder builder = parentTraceState.toBuilder();
          builder.put(SAMPLING_GEOMETRIC_RANDOM_VALUE_KEY, Integer.toString(geometricRandomValue));
          builder.put(SAMPLING_RATE_EXPONENT_KEY, Integer.toString(samplingRateExponent));

          if (recordingMode.collectAncestorDistance()) {
            String numberDroppedParentsAsString =
                parentTraceState.get(NUMBER_DROPPED_ANCESTORS_KEY);
            long numberDroppedAncestors =
                (numberDroppedParentsAsString != null)
                    ? Long.parseLong(numberDroppedParentsAsString)
                    : 0;
            builder.put(NUMBER_DROPPED_ANCESTORS_KEY, Long.toString(numberDroppedAncestors + 1));
          } else {
            builder.remove(NUMBER_DROPPED_ANCESTORS_KEY);
          }

          if (recordingMode.collectAncestorLink()) {
            String sampledAncestorSpanId = parentTraceState.get(SAMPLED_ANCESTOR_SPAN_ID_KEY);
            if (sampledAncestorSpanId == null) {
              sampledAncestorSpanId = parentSpanId;
            }
            builder.put(SAMPLED_ANCESTOR_SPAN_ID_KEY, sampledAncestorSpanId);
          } else {
            builder.remove(SAMPLED_ANCESTOR_SPAN_ID_KEY);
          }

          return builder.build();
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
