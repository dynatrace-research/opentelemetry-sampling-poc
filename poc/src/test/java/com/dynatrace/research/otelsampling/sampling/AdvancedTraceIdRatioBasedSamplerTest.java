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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.verifyNoInteractions;

import com.dynatrace.research.otelsampling.simulation.DeterministicIdGenerator;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.*;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.ContextKey;
import io.opentelemetry.sdk.trace.data.LinkData;
import io.opentelemetry.sdk.trace.samplers.SamplingDecision;
import io.opentelemetry.sdk.trace.samplers.SamplingResult;
import java.util.Collections;
import java.util.List;
import org.hipparchus.stat.inference.AlternativeHypothesis;
import org.hipparchus.stat.inference.BinomialTest;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class AdvancedTraceIdRatioBasedSamplerTest {

  private Context parentContext;
  private String traceId;
  private String name;
  private SpanKind spanKind;
  private Attributes attributes;
  private List<LinkData> parentLinks;

  @Before
  public void init() {

    parentContext = Context.root();
    traceId = "0123456789abcdef0123456789abcdef";
    name = "name";
    spanKind = SpanKind.SERVER;
    attributes = Attributes.empty();
    parentLinks = Collections.emptyList();
  }

  @Test
  public void testVariousRatios() {

    int numCycles = 10000;
    double alpha = 0.01;
    double[] ratios = {0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0};

    for (RecordingMode mode : RecordingMode.values()) {

      DeterministicIdGenerator idGenerator = new DeterministicIdGenerator(0L);
      AdvancedTraceIdRatioBasedSampler sampler = AdvancedTraceIdRatioBasedSampler.create(mode);

      for (double ratio : ratios) {
        int recordCounter = 0;
        sampler.setRatio(ratio);
        for (long i = 0; i < numCycles; ++i) {
          traceId = idGenerator.generateTraceId();
          SamplingResult samplingResult =
              sampler.shouldSample(parentContext, traceId, name, spanKind, attributes, parentLinks);
          if (samplingResult.getDecision() == SamplingDecision.RECORD_AND_SAMPLE) {
            recordCounter += 1;
            assertEquals(
                ratio,
                samplingResult
                    .getAttributes()
                    .get(
                        AttributeKey.doubleKey(
                            AdvancedTraceIdRatioBasedSampler.SAMPLING_RATIO_KEY)),
                0d);
          }
        }
        assertFalse(
            new BinomialTest()
                .binomialTest(
                    numCycles, recordCounter, ratio, AlternativeHypothesis.TWO_SIDED, alpha));
      }
    }
  }

  @Test
  public void testParentSampledAndChildSampledAncestorLinkAndDistance() {

    for (RecordingMode mode : RecordingMode.values()) {

      AdvancedTraceIdRatioBasedSampler sampler = AdvancedTraceIdRatioBasedSampler.create(mode);

      TraceState parentTraceState = TraceState.builder().build();
      String parentSpanId = "0123456789abcdef";

      parentContext = Mockito.mock(Context.class);

      sampler.setRatio(1);

      SamplingResult samplingResult =
          sampler.shouldSample(parentContext, traceId, name, spanKind, attributes, parentLinks);

      TraceState traceState = samplingResult.getUpdatedTraceState(parentTraceState);

      verifyNoInteractions(parentContext);
      assertEquals(SamplingDecision.RECORD_AND_SAMPLE, samplingResult.getDecision());
      assertEquals(
          1,
          samplingResult
              .getAttributes()
              .get(AttributeKey.doubleKey(AdvancedTraceIdRatioBasedSampler.SAMPLING_RATIO_KEY)),
          0d);
      assertEquals(
          mode,
          RecordingMode.valueOf(
              samplingResult
                  .getAttributes()
                  .get(AttributeKey.stringKey(AdvancedTraceIdRatioBasedSampler.SAMPLING_MODE))));
      assertNull(traceState.get(AdvancedTraceIdRatioBasedSampler.NUMBER_DROPPED_ANCESTORS_KEY));
      assertNull(
          parentSpanId,
          traceState.get(AdvancedTraceIdRatioBasedSampler.SAMPLED_ANCESTOR_SPAN_ID_KEY));
    }
  }

  @Test
  public void testParentSampledAndChildNotSampled() {

    for (RecordingMode mode : RecordingMode.values()) {

      AdvancedTraceIdRatioBasedSampler sampler = AdvancedTraceIdRatioBasedSampler.create(mode);

      TraceState parentTraceState = TraceState.builder().build();
      String parentSpanId = "0123456789abcdef";

      parentContext =
          new Context() {

            @Override
            public <V> Context with(ContextKey<V> k1, V v1) {
              fail();
              return null;
            }

            @Override
            public <V> V get(ContextKey<V> key) {
              if ("opentelemetry-trace-span-key".equals(key.toString())) {
                @SuppressWarnings("unchecked")
                V result =
                    (V)
                        Span.wrap(
                            SpanContext.create(
                                traceId, parentSpanId, TraceFlags.getDefault(), parentTraceState));
                return result;
              } else {
                fail();
                return null;
              }
            }
          };

      sampler.setRatio(0);

      SamplingResult samplingResult =
          sampler.shouldSample(parentContext, traceId, name, spanKind, attributes, parentLinks);

      TraceState traceState = samplingResult.getUpdatedTraceState(parentTraceState);

      assertEquals(SamplingDecision.DROP, samplingResult.getDecision());

      if (mode.collectAncestorDistance()) {
        assertEquals(
            "1", traceState.get(AdvancedTraceIdRatioBasedSampler.NUMBER_DROPPED_ANCESTORS_KEY));
      }
      if (mode.collectAncestorLink())
        assertEquals(
            parentSpanId,
            traceState.get(AdvancedTraceIdRatioBasedSampler.SAMPLED_ANCESTOR_SPAN_ID_KEY));
    }
  }

  @Test
  public void testParentNotSampledAndChildSampled() {

    for (RecordingMode mode : RecordingMode.values()) {

      AdvancedTraceIdRatioBasedSampler sampler = AdvancedTraceIdRatioBasedSampler.create(mode);

      String grandParentSpanId = "fedcba9876543210";

      TraceState parentTraceState =
          TraceState.builder()
              .put(AdvancedTraceIdRatioBasedSampler.NUMBER_DROPPED_ANCESTORS_KEY, "12")
              .put(AdvancedTraceIdRatioBasedSampler.SAMPLED_ANCESTOR_SPAN_ID_KEY, grandParentSpanId)
              .build();

      parentContext = Mockito.mock(Context.class);

      sampler.setRatio(1);

      SamplingResult samplingResult =
          sampler.shouldSample(parentContext, traceId, name, spanKind, attributes, parentLinks);

      TraceState traceState = samplingResult.getUpdatedTraceState(parentTraceState);

      verifyNoInteractions(parentContext);
      assertEquals(SamplingDecision.RECORD_AND_SAMPLE, samplingResult.getDecision());
      assertEquals(
          1,
          samplingResult
              .getAttributes()
              .get(AttributeKey.doubleKey(AdvancedTraceIdRatioBasedSampler.SAMPLING_RATIO_KEY)),
          0d);
      assertNull(traceState.get(AdvancedTraceIdRatioBasedSampler.NUMBER_DROPPED_ANCESTORS_KEY));
      assertNull(traceState.get(AdvancedTraceIdRatioBasedSampler.SAMPLED_ANCESTOR_SPAN_ID_KEY));
    }
  }

  @Test
  public void testParentNotSampledAndChildNotSampled() {

    for (RecordingMode mode : RecordingMode.values()) {

      AdvancedTraceIdRatioBasedSampler sampler = AdvancedTraceIdRatioBasedSampler.create(mode);

      String grandParentSpanId = "fedcba9876543210";
      String parentSpanId = "0123456789abcdef";

      TraceState parentTraceState =
          TraceState.builder()
              .put(AdvancedTraceIdRatioBasedSampler.NUMBER_DROPPED_ANCESTORS_KEY, "12")
              .put(AdvancedTraceIdRatioBasedSampler.SAMPLED_ANCESTOR_SPAN_ID_KEY, grandParentSpanId)
              .build();

      parentContext =
          new Context() {

            @Override
            public <V> Context with(ContextKey<V> k1, V v1) {
              fail();
              return null;
            }

            @Override
            public <V> V get(ContextKey<V> key) {
              if ("opentelemetry-trace-span-key".equals(key.toString())) {
                @SuppressWarnings("unchecked")
                V result =
                    (V)
                        Span.wrap(
                            SpanContext.create(
                                traceId, parentSpanId, TraceFlags.getDefault(), parentTraceState));
                return result;
              } else {
                fail();
                return null;
              }
            }
          };

      sampler.setRatio(0);

      SamplingResult samplingResult =
          sampler.shouldSample(parentContext, traceId, name, spanKind, attributes, parentLinks);

      TraceState traceState = samplingResult.getUpdatedTraceState(parentTraceState);

      assertEquals(SamplingDecision.DROP, samplingResult.getDecision());
      if (mode.collectAncestorDistance()) {
        assertEquals(
            "13", traceState.get(AdvancedTraceIdRatioBasedSampler.NUMBER_DROPPED_ANCESTORS_KEY));
      } else {
        assertNull(traceState.get(AdvancedTraceIdRatioBasedSampler.NUMBER_DROPPED_ANCESTORS_KEY));
      }
      if (mode.collectAncestorLink()) {
        assertEquals(
            grandParentSpanId,
            traceState.get(AdvancedTraceIdRatioBasedSampler.SAMPLED_ANCESTOR_SPAN_ID_KEY));
      } else {
        assertNull(traceState.get(AdvancedTraceIdRatioBasedSampler.SAMPLED_ANCESTOR_SPAN_ID_KEY));
      }
    }
  }
}
