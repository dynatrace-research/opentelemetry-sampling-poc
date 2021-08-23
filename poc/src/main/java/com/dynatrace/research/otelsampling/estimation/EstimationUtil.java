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
package com.dynatrace.research.otelsampling.estimation;

import static com.google.common.base.Preconditions.checkArgument;

import com.dynatrace.research.otelsampling.sampling.SamplingUtil;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class EstimationUtil {

  private EstimationUtil() {}

  private static final Object DUMMY_KEY = new Object();
  private static final Double ZERO = 0.;

  public static double estimate(
      ScalarQuantityExtractor scalarQuantityExtractor, Collection<SpanData> spanData) {
    return estimate(
            VectorQuantityExtractor.of(
                Collections.singletonMap(DUMMY_KEY, scalarQuantityExtractor)),
            spanData)
        .getOrDefault(DUMMY_KEY, ZERO);
  }

  public static <T> Map<T, Double> estimate(
      VectorQuantityExtractor<T> vectorQuantityExtractor, Collection<SpanData> spanData) {

    if (spanData.isEmpty()) return Collections.emptyMap();

    // check if all spans belong to the same trace
    checkArgument(spanData.stream().map(SpanData::getTraceId).distinct().count() == 1);

    Map<T, Double> q = new HashMap<>();
    Map<T, Double> qPrev = new HashMap<>();
    Map<T, Double> qPrevFinal = qPrev;
    vectorQuantityExtractor.extract(spanData, (key, quantity) -> qPrevFinal.put(key, quantity));
    while (true) {
      final double v =
          spanData.stream().mapToDouble(SamplingUtil::getSamplingRatio).min().getAsDouble();
      final double vReciprocal = 1. / v;

      spanData = SamplingUtil.downSample(spanData, v);

      if (spanData.isEmpty()) {
        qPrev.forEach((key, qPrevElement) -> q.merge(key, qPrevElement * vReciprocal, Double::sum));
        return q;
      }

      Map<T, Double> qNext = new HashMap<>();
      vectorQuantityExtractor.extract(spanData, (key, quantity) -> qNext.put(key, quantity));

      Map<T, Double> qDifferences = qPrev;
      qNext.forEach(
          (key, valNext) ->
              qDifferences.compute(
                  key, (k, valPrev) -> ((valPrev != null) ? valPrev : 0) - valNext));
      qDifferences.forEach((key, valDiff) -> q.merge(key, valDiff * vReciprocal, Double::sum));

      qPrev = qNext;
    }
  }
}
