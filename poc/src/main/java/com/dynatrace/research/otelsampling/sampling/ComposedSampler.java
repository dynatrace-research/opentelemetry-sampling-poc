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
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.trace.data.LinkData;
import java.util.List;

/*
 * This sampler samples if at least one of its component samplers would sample.
 */
public class ComposedSampler extends AbstractConsistentSampler {

  private final AbstractConsistentSampler sampler1;
  private final AbstractConsistentSampler sampler2;

  public ComposedSampler(AbstractConsistentSampler sampler1, AbstractConsistentSampler sampler2) {
    this.sampler1 = requireNonNull(sampler1);
    this.sampler2 = requireNonNull(sampler2);
  }

  @Override
  protected int getSamplingRateExponent(
      Context parentContext,
      String traceId,
      String name,
      SpanKind spanKind,
      Attributes attributes,
      List<LinkData> parentLinks) {
    return Math.min(
        sampler1.getSamplingRateExponent(
            parentContext, traceId, name, spanKind, attributes, parentLinks),
        sampler2.getSamplingRateExponent(
            parentContext, traceId, name, spanKind, attributes, parentLinks));
  }

  @Override
  public String getDescription() {
    return "ComposedSampler{"
        + "sampler1="
        + sampler1.getDescription()
        + ", sampler2="
        + sampler2.getDescription()
        + '}';
  }
}
