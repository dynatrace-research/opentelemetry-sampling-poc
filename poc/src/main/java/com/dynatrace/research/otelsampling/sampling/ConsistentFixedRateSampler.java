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
import java.util.function.BooleanSupplier;

public class ConsistentFixedRateSampler extends AbstractConsistentSampler {

  private final int lowerBoundExponent;
  private final int upperBoundExponent;
  private final double probabilityToUseLowerBoundExponent;
  private final double samplingRate;

  public ConsistentFixedRateSampler(
      double samplingRate, BooleanSupplier threadSafeRandomGenerator) {
    super(threadSafeRandomGenerator);
    if (samplingRate < 0. || samplingRate > 1.) {
      throw new IllegalArgumentException("Sampling rate must be in the range [0,1]!");
    }
    this.samplingRate = samplingRate;

    int l = 1;
    while (l < 63 && getSamplingRate(l + 1) >= samplingRate) {
      l += 1;
    }
    int u = 63;
    while (u > 1 && getSamplingRate(u - 1) <= samplingRate) {
      u -= 1;
    }
    lowerBoundExponent = l;
    upperBoundExponent = u;

    if (getSamplingRate(lowerBoundExponent) < samplingRate) {
      throw new IllegalStateException();
    }
    if (getSamplingRate(upperBoundExponent) > samplingRate) {
      throw new IllegalStateException();
    }
    if (lowerBoundExponent > upperBoundExponent) {
      throw new IllegalStateException();
    }
    if (lowerBoundExponent + 1 < upperBoundExponent) {
      throw new IllegalStateException();
    }

    if (lowerBoundExponent == upperBoundExponent) {
      probabilityToUseLowerBoundExponent = 1;
    } else {
      double upperSamplingRate = getSamplingRate(lowerBoundExponent);
      double lowerSamplingRate = getSamplingRate(upperBoundExponent);
      probabilityToUseLowerBoundExponent =
          (samplingRate - lowerSamplingRate) / (upperSamplingRate - lowerSamplingRate);
    }
  }

  private boolean doBernoulliTrial(double successProbability) {
    while (true) {
      if (successProbability == 0) return false;
      if (successProbability == 1) return true;
      boolean b = successProbability > 0.5;
      if (threadSafeRandomGenerator.getAsBoolean()) return b;
      successProbability += successProbability;
      if (b) successProbability -= 1;
    }
  }

  @Override
  protected int getSamplingRateExponent(
      Context parentContext,
      String traceId,
      String name,
      SpanKind spanKind,
      Attributes attributes,
      List<LinkData> parentLinks) {
    if (doBernoulliTrial(probabilityToUseLowerBoundExponent)) {
      return lowerBoundExponent;
    } else {
      return upperBoundExponent;
    }
  }

  @Override
  public final String getDescription() {
    return "ConsistentFixedRateSampler{" + "samplingRate=" + samplingRate + '}';
  }
}
