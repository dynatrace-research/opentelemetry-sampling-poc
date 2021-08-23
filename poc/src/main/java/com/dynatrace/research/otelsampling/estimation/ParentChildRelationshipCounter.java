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

import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

import com.dynatrace.research.otelsampling.sampling.SamplingUtil;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.util.Collection;
import java.util.Map;
import java.util.function.Predicate;

public class ParentChildRelationshipCounter implements ScalarQuantityExtractor {

  private final Predicate<? super SpanData> parentSpanMatcher;
  private final Predicate<? super SpanData> childSpanMatcher;

  public ParentChildRelationshipCounter(
      Predicate<? super SpanData> parentSpanMatcher, Predicate<? super SpanData> childSpanMatcher) {
    this.parentSpanMatcher = requireNonNull(parentSpanMatcher);
    this.childSpanMatcher = requireNonNull(childSpanMatcher);
  }

  @Override
  public double extract(Collection<? extends SpanData> spanData) {
    Map<String, SpanData> spanIndex =
        spanData.stream().collect(toMap(SpanData::getSpanId, identity()));

    long result = 0;
    for (SpanData span : spanData) {
      if (!childSpanMatcher.test(span)) continue;

      SpanData parentSpan = spanIndex.get(SamplingUtil.getParentSpanId(span));
      while (parentSpan != null) {
        if (parentSpanMatcher.test(parentSpan)) {
          result += 1;
          break;
        }
        parentSpan = spanIndex.get(SamplingUtil.getParentSpanId(parentSpan));
      }
    }
    return result;
  }
}
