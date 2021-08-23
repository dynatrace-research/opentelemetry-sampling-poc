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

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.stream.Collectors.toList;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.util.Collection;
import java.util.Collections;

public final class SamplingUtil {

  private SamplingUtil() {}

  /**
   * Down samples the given collection of span data. Only span data with sampling rates greater than
   * the given sample rate threshold will be kept and returned as new collection.
   *
   * @param spanData a collection of span data
   * @param sampleRateThreshold the sample rate threshold
   * @param <T>
   * @return the new down-sampled collection of span data
   */
  public static final <T extends SpanData> Collection<T> downSample(
      Collection<T> spanData, double sampleRateThreshold) {

    if (spanData.isEmpty()) return Collections.emptyList();

    // verify that all span data has been collected with the same sampling mode
    SamplingMode samplingMode = getSamplingMode(spanData.stream().findFirst().get());
    checkArgument(spanData.stream().allMatch(s -> getSamplingMode(s) == samplingMode));

    // TODO support also other sampling modes
    checkArgument(samplingMode == SamplingMode.PARENT_LINK);

    return spanData.stream()
        .filter(s -> getSamplingRatio(s) > sampleRateThreshold)
        .collect(toList());
  }

  public static int getParentDistance(SpanData spanData) {
    String v =
        spanData
            .getParentSpanContext()
            .getTraceState()
            .get(AdvancedTraceIdRatioBasedSampler.NUMBER_DROPPED_ANCESTORS_KEY);
    if (v == null) {
      return 0;
    } else {
      return Integer.parseInt(v);
    }
  }

  public static String getParentSpanId(SpanData spanData) {
    String v =
        spanData
            .getParentSpanContext()
            .getTraceState()
            .get(AdvancedTraceIdRatioBasedSampler.SAMPLED_ANCESTOR_SPAN_ID_KEY);
    if (v == null) {
      return spanData.getParentSpanId();
    } else {
      return v;
    }
  }

  public static double getSamplingRatio(SpanData spanData) {
    Double v =
        spanData
            .getAttributes()
            .get(AttributeKey.doubleKey(AdvancedTraceIdRatioBasedSampler.SAMPLING_RATIO_KEY));
    if (v == null) {
      return Double.NaN;
    } else {
      return v;
    }
  }

  public static SamplingMode getSamplingMode(SpanData spanData) {
    String s =
        spanData
            .getAttributes()
            .get(AttributeKey.stringKey(AdvancedTraceIdRatioBasedSampler.SAMPLING_MODE));
    if (s == null) {
      return null;
    } else {
      return SamplingMode.valueOf(s);
    }
  }
}
