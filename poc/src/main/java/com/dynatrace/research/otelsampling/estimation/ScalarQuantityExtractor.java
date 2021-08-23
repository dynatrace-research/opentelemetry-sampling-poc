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

import io.opentelemetry.sdk.trace.data.SpanData;
import java.util.Collection;
import java.util.function.Predicate;

@FunctionalInterface
public interface ScalarQuantityExtractor {

  /**
   * @param spanData collection of span data
   * @return extracts the quantity from a collection of spans belonging to the same trace
   */
  double extract(Collection<? extends SpanData> spanData);

  static ScalarQuantityExtractor countMatchingSpans(Predicate<? super SpanData> spanPredicate) {
    return spanData -> spanData.stream().filter(spanPredicate).count();
  }

  static ScalarQuantityExtractor countMatchingTraces(
      Predicate<? super Collection<? extends SpanData>> tracePredicate) {
    return spanData -> tracePredicate.test(spanData) ? 1 : 0;
  }
}
