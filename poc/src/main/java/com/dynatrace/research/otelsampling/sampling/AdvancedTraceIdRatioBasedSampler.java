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

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.internal.OtelEncodingUtils;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.api.trace.TraceStateBuilder;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.trace.data.LinkData;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.sdk.trace.samplers.SamplingDecision;
import io.opentelemetry.sdk.trace.samplers.SamplingResult;
import java.util.List;
import javax.annotation.concurrent.Immutable;

public final class AdvancedTraceIdRatioBasedSampler implements Sampler {

  @Immutable
  private static class RatioSpecificData {
    private final double ratio;
    private final long idUpperBound;
    private final Attributes attributes;

    private RatioSpecificData(double ratio, SamplingMode mode) {
      if (ratio < 0.0 || ratio > 1.0) {
        throw new IllegalArgumentException("ratio must be in range [0.0, 1.0]");
      }
      this.ratio = ratio;
      this.idUpperBound = calculateIdUpperBound(ratio);
      this.attributes =
          Attributes.of(
              AttributeKey.doubleKey(SAMPLING_RATIO_KEY),
              ratio,
              AttributeKey.stringKey(SAMPLING_MODE),
              mode.toString());
    }

    private static long calculateIdUpperBound(double ratio) {
      long idUpperBound;
      // Special case the limits, to avoid any possible issues with lack of precision across
      // double/long boundaries. For probability == 0.0, we use Long.MIN_VALUE as this guarantees
      // that we will never sample a trace, even in the case where the id == Long.MIN_VALUE, since
      // Math.Abs(Long.MIN_VALUE) == Long.MIN_VALUE.
      if (ratio == 0.0) {
        idUpperBound = Long.MIN_VALUE;
      } else if (ratio == 1.0) {
        idUpperBound = Long.MAX_VALUE;
      } else {
        idUpperBound = (long) (ratio * Long.MAX_VALUE);
      }
      return idUpperBound;
    }
  }

  private volatile RatioSpecificData ratioSpecificData;

  private final SamplingMode mode;

  public static final String NUMBER_DROPPED_ANCESTORS_KEY = "number-dropped-ancestors";
  public static final String SAMPLED_ANCESTOR_SPAN_ID_KEY = "sampled-ancestor-span-id";
  public static final String SAMPLING_RATIO_KEY = "sampling-ratio";
  public static final String SAMPLING_MODE = "sampling-mode";

  public static AdvancedTraceIdRatioBasedSampler create(SamplingMode mode) {
    return new AdvancedTraceIdRatioBasedSampler(mode, 1.0);
  }

  public static AdvancedTraceIdRatioBasedSampler create(SamplingMode mode, double ratio) {
    return new AdvancedTraceIdRatioBasedSampler(mode, ratio);
  }

  private AdvancedTraceIdRatioBasedSampler(SamplingMode mode, double ratio) {
    this.mode = mode;
    this.ratioSpecificData = new RatioSpecificData(ratio, mode);
  }

  public void setRatio(double ratio) {
    this.ratioSpecificData = new RatioSpecificData(ratio, mode);
  }

  public double getRatio() {
    return ratioSpecificData.ratio;
  }

  private static long getTraceIdRandomPart(String traceId) {
    return OtelEncodingUtils.longFromBase16String(traceId, 16);
  }

  @Override
  public final SamplingResult shouldSample(
      Context parentContext,
      String traceId,
      String name,
      SpanKind spanKind,
      Attributes attributes,
      List<LinkData> parentLinks) {

    // use local copy to make sure that the ratio is consistent within this method, even if the
    // ratio is set to a new value by a different thread at the same time
    final RatioSpecificData ratioSpecificDataCopy = ratioSpecificData;

    // Always sample if we are within probability range. This is true even for child spans (that
    // may have had a different sampling samplingResult made) to allow for different sampling
    // policies, and dynamic increases to sampling probabilities for debugging purposes.
    // Note use of '<' for comparison. This ensures that we never sample for probability == 0.0,
    // while allowing for a (very) small chance of *not* sampling if the id == Long.MAX_VALUE.
    // This is considered a reasonable tradeoff for the simplicity/performance requirements (this
    // code is executed in-line for every Span creation).
    boolean shouldSample =
        Math.abs(getTraceIdRandomPart(traceId)) < ratioSpecificDataCopy.idUpperBound;

    if (shouldSample) {
      return new SamplingResult() {

        @Override
        public SamplingDecision getDecision() {
          return SamplingDecision.RECORD_AND_SAMPLE;
        }

        @Override
        public Attributes getAttributes() {
          return ratioSpecificDataCopy.attributes;
        }

        @Override
        public TraceState getUpdatedTraceState(TraceState parentTraceState) {
          if (parentTraceState.get(NUMBER_DROPPED_ANCESTORS_KEY) == null
              && parentTraceState.get(SAMPLED_ANCESTOR_SPAN_ID_KEY) == null) {
            return parentTraceState;
          } else {
            return parentTraceState.toBuilder()
                .remove(NUMBER_DROPPED_ANCESTORS_KEY)
                .remove(SAMPLED_ANCESTOR_SPAN_ID_KEY)
                .build();
          }
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
          String numberDroppedParentsAsString = parentTraceState.get(NUMBER_DROPPED_ANCESTORS_KEY);
          long numberDroppedAncestors =
              (numberDroppedParentsAsString != null)
                  ? Long.parseLong(numberDroppedParentsAsString)
                  : 0;

          String sampledAncestorSpanId = parentTraceState.get(SAMPLED_ANCESTOR_SPAN_ID_KEY);
          if (sampledAncestorSpanId == null) {
            sampledAncestorSpanId = parentSpanId;
          }

          TraceStateBuilder parentTraceStateBuilder = parentTraceState.toBuilder();
          if (mode.collectAncestorLink()) {
            parentTraceStateBuilder.put(SAMPLED_ANCESTOR_SPAN_ID_KEY, sampledAncestorSpanId);
          } else {
            parentTraceStateBuilder.remove(SAMPLED_ANCESTOR_SPAN_ID_KEY);
          }
          if (mode.collectAncestorDistance()) {
            parentTraceStateBuilder.put(
                NUMBER_DROPPED_ANCESTORS_KEY, Long.toString(numberDroppedAncestors + 1));
          } else {
            parentTraceStateBuilder.remove(NUMBER_DROPPED_ANCESTORS_KEY);
          }
          return parentTraceStateBuilder.build();
        }
      };
    }
  }

  @Override
  public final String getDescription() {
    return "AdvancedTraceIdRatioBasedSampler{"
        + "ratioSpecificData="
        + ratioSpecificData
        + ", mode="
        + mode
        + '}';
  }

  @Override
  public String toString() {
    return getDescription();
  }
}
