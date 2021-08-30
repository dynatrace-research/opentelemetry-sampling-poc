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

import static com.dynatrace.research.otelsampling.sampling.AbstractConsistentSampler.NUMBER_DROPPED_ANCESTORS_KEY;
import static com.dynatrace.research.otelsampling.sampling.AbstractConsistentSampler.SAMPLED_ANCESTOR_SPAN_ID_KEY;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

import com.dynatrace.research.otelsampling.simulation.TraceUtil;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.*;
import io.opentelemetry.sdk.common.InstrumentationLibraryInfo;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.data.EventData;
import io.opentelemetry.sdk.trace.data.LinkData;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.data.StatusData;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

public final class SamplingUtil {

  private SamplingUtil() {}

  /**
   * Down samples the given collection of span data. Only span data with sampling rates greater than
   * the given sample rate threshold will be kept and returned as new collection.
   *
   * @param spanData a collection of span data
   * @param sampleRateThreshold the sample rate threshold
   * @return the new down-sampled collection of span data
   */
  public static Collection<SpanData> downSample(
      Collection<SpanData> spanData, double sampleRateThreshold) {

    if (spanData.isEmpty()) return Collections.emptyList();

    Predicate<SpanData> predicate = s -> getSamplingRatio(s) > sampleRateThreshold;

    Map<String, SpanData> index = TraceUtil.createSpanDataIndex(spanData);

    return spanData.stream()
        .filter(predicate)
        .map(s -> updateAncestorInformation(s, index, predicate))
        .collect(toList());
  }

  private static SpanData updateAncestorInformation(
      SpanData s, Map<String, SpanData> index, Predicate<SpanData> predicate) {
    SpanData newAncestorSpan = index.get(getAncestorSpanId(s));
    int newNumberDroppedAncestors = getNumberDroppedAncestors(s);
    while (newAncestorSpan != null && !predicate.test(newAncestorSpan)) {
      newNumberDroppedAncestors += getNumberDroppedAncestors(newAncestorSpan);
      newAncestorSpan = index.get(getAncestorSpanId(newAncestorSpan));
    }
    return SpanDataWithModifiedAncestorData.create(
        s,
        (newAncestorSpan != null) ? newAncestorSpan.getSpanId() : SpanId.getInvalid(),
        newNumberDroppedAncestors);
  }

  public static int getNumberDroppedAncestors(SpanData spanData) {
    String v =
        spanData
            .getParentSpanContext()
            .getTraceState()
            .get(AbstractConsistentSampler.NUMBER_DROPPED_ANCESTORS_KEY);
    if (v == null) {
      return 0;
    } else {
      return Integer.parseInt(v);
    }
  }

  public static String getAncestorSpanId(SpanData spanData) {
    String v =
        spanData
            .getParentSpanContext()
            .getTraceState()
            .get(AbstractConsistentSampler.SAMPLED_ANCESTOR_SPAN_ID_KEY);
    if (v == null) {
      return spanData.getParentSpanId();
    } else {
      return v;
    }
  }

  public static double getSamplingRatio(SpanData spanData) {
    TraceState traceState = spanData.getSpanContext().getTraceState();
    String pow2ParentSaplingRateAsString =
        traceState.get(AbstractConsistentSampler.SAMPLING_RATE_EXPONENT_KEY);
    if (pow2ParentSaplingRateAsString != null) {
      int parentSamplingRateExponent =
          Integer.parseInt(pow2ParentSaplingRateAsString); // TODO exception handling
      if (parentSamplingRateExponent >= 0 && parentSamplingRateExponent <= 63) {
        return 1. / (1 << (parentSamplingRateExponent - 1));
      }
    }
    return Double.NaN;
  }

  private static final class SpanDataWithModifiedAncestorData implements SpanData {

    private final SpanData delegate;
    private final SpanContext newSpanContext;

    private SpanDataWithModifiedAncestorData(
        SpanData spanData, String newAncestorSpanId, int newNumDroppedAncestors) {
      this.delegate = requireNonNull(spanData);

      SpanContext parentSpanContext = spanData.getParentSpanContext();

      TraceStateBuilder builder = parentSpanContext.getTraceState().toBuilder();
      if (!newAncestorSpanId.equals(parentSpanContext.getSpanId())) {
        builder.put(SAMPLED_ANCESTOR_SPAN_ID_KEY, newAncestorSpanId);
      } else {
        builder.remove(SAMPLED_ANCESTOR_SPAN_ID_KEY);
      }
      if (newNumDroppedAncestors > 0) {
        builder.put(NUMBER_DROPPED_ANCESTORS_KEY, Integer.toString(newNumDroppedAncestors));
      } else {
        builder.remove(NUMBER_DROPPED_ANCESTORS_KEY);
      }
      TraceState traceState = builder.build();
      newSpanContext =
          new SpanContext() {
            @Override
            public String getTraceId() {
              return parentSpanContext.getTraceId();
            }

            @Override
            public String getSpanId() {
              return parentSpanContext.getSpanId();
            }

            @Override
            public TraceFlags getTraceFlags() {
              return parentSpanContext.getTraceFlags();
            }

            @Override
            public TraceState getTraceState() {
              return traceState;
            }

            @Override
            public boolean isRemote() {
              return parentSpanContext.isRemote();
            }
          };
    }

    private static SpanData create(
        SpanData spanData, String newAncestorSpanId, int newNumDroppedAncestors) {
      requireNonNull(spanData);
      requireNonNull(newAncestorSpanId);
      if (spanData.getParentSpanId().equals(SpanId.getInvalid())) {
        return spanData;
      }
      if (spanData instanceof SpanDataWithModifiedAncestorData) {
        return new SpanDataWithModifiedAncestorData(
            ((SpanDataWithModifiedAncestorData) spanData).delegate,
            newAncestorSpanId,
            newNumDroppedAncestors);
      } else {
        return new SpanDataWithModifiedAncestorData(
            spanData, newAncestorSpanId, newNumDroppedAncestors);
      }
    }

    @Override
    public String getName() {
      return delegate.getName();
    }

    @Override
    public SpanKind getKind() {
      return delegate.getKind();
    }

    @Override
    public SpanContext getSpanContext() {
      return delegate.getSpanContext();
    }

    @Override
    public String getTraceId() {
      return delegate.getTraceId();
    }

    @Override
    public String getSpanId() {
      return delegate.getSpanId();
    }

    @Override
    public SpanContext getParentSpanContext() {
      return newSpanContext;
    }

    @Override
    public String getParentSpanId() {
      return delegate.getParentSpanId();
    }

    @Override
    public StatusData getStatus() {
      return delegate.getStatus();
    }

    @Override
    public long getStartEpochNanos() {
      return delegate.getStartEpochNanos();
    }

    @Override
    public Attributes getAttributes() {
      return delegate.getAttributes();
    }

    @Override
    public List<EventData> getEvents() {
      return delegate.getEvents();
    }

    @Override
    public List<LinkData> getLinks() {
      return delegate.getLinks();
    }

    @Override
    public long getEndEpochNanos() {
      return delegate.getEndEpochNanos();
    }

    @Override
    public boolean hasEnded() {
      return delegate.hasEnded();
    }

    @Override
    public int getTotalRecordedEvents() {
      return delegate.getTotalRecordedEvents();
    }

    @Override
    public int getTotalRecordedLinks() {
      return delegate.getTotalRecordedLinks();
    }

    @Override
    public int getTotalAttributeCount() {
      return delegate.getTotalAttributeCount();
    }

    @Override
    public InstrumentationLibraryInfo getInstrumentationLibraryInfo() {
      return delegate.getInstrumentationLibraryInfo();
    }

    @Override
    public Resource getResource() {
      return delegate.getResource();
    }
  }
}
