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
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.trace.data.LinkData;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/*
 * This sampler does not sample for a predefined period since the last sampling.
 */
public class SkipPeriodSampler extends AbstractConsistentSampler {

  private final AtomicLong nextSamplingTimeMillis;
  private final long periodMillis;

  private static final int ONE_SAMPLING_RATE = 1;
  private static final int ZERO_SAMPLING_RATE = 63;

  public SkipPeriodSampler(long periodMillis) {
    this.nextSamplingTimeMillis = new AtomicLong(getNow());
    this.periodMillis = periodMillis;
  }

  private static long getNow() {
    return System.currentTimeMillis();
  }

  @Override
  protected int getSamplingRateExponent(
      Context parentContext,
      String traceId,
      String name,
      SpanKind spanKind,
      Attributes attributes,
      List<LinkData> parentLinks) {
    long now = getNow();
    long currentNextSamplingTimeMillis = nextSamplingTimeMillis.get();
    while (now >= currentNextSamplingTimeMillis) {
      boolean updated =
          nextSamplingTimeMillis.compareAndSet(
              currentNextSamplingTimeMillis, Math.addExact(now, periodMillis));
      if (updated) return ONE_SAMPLING_RATE;
      currentNextSamplingTimeMillis = nextSamplingTimeMillis.get();
    }
    return ZERO_SAMPLING_RATE;
  }

  @Override
  public String getDescription() {
    return "SkipPeriodSampler{" + "periodMillis=" + periodMillis + '}';
  }
}
