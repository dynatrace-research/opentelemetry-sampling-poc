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
package com.dynatrace.research.otelsampling.simulation;

import static java.util.Objects.requireNonNull;

import com.google.common.hash.Hashing;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.IdGenerator;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import javax.annotation.CheckForNull;

public class InstrumentedServiceImpl implements InstrumentedService {

  private final String id;
  private final IdGenerator idGenerator;
  private SdkTracerProvider sdkTracerProvider;
  private OpenTelemetry openTelemetry;
  private final SpanExporter spanExporter;

  InstrumentedServiceImpl(String id, long hashSalt, SpanExporter spanExporter) {

    requireNonNull(id);
    requireNonNull(spanExporter);

    long seed =
        Hashing.murmur3_128().newHasher().putLong(hashSalt).putUnencodedChars(id).hash().asLong();

    this.idGenerator = new DeterministicIdGenerator(seed);
    this.spanExporter = requireNonNull(spanExporter);
    this.id = requireNonNull(id);

    setSampler(Sampler.alwaysOn());
  }

  @Override
  public synchronized CallContext call(@CheckForNull CallContext callContext) {

    Tracer tracer = openTelemetry.getTracer("instrumentation@" + id);
    SpanBuilder spanBuilder = tracer.spanBuilder("span@" + id);
    if (callContext != null) {
      spanBuilder.setParent(Context.current().with(Span.wrap(callContext.getSpanContext())));
    }
    Span span = spanBuilder.startSpan();
    return new CallContext() {
      @Override
      public void close() {
        span.end();
      }

      @Override
      public SpanContext getSpanContext() {
        return span.getSpanContext();
      }
    };
  }

  @Override
  public synchronized void setSampler(Sampler sampler) {

    requireNonNull(sampler);

    sdkTracerProvider =
        SdkTracerProvider.builder()
            .setIdGenerator(idGenerator)
            .setSampler(sampler)
            .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
            .build();

    openTelemetry = OpenTelemetrySdk.builder().setTracerProvider(sdkTracerProvider).build();
  }
}
