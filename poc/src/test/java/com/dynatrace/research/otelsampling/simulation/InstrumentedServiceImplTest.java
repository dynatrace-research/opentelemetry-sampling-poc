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

import static org.junit.Assert.assertEquals;

import com.dynatrace.research.otelsampling.exporter.CollectingSpanExporter;
import com.dynatrace.research.otelsampling.simulation.InstrumentedService.CallContext;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.util.List;
import org.junit.Test;

public class InstrumentedServiceImplTest {

  @Test
  public void test() {

    CollectingSpanExporter exporter = new CollectingSpanExporter();

    InstrumentedServiceImpl instrumentedService1 = new InstrumentedServiceImpl("id1", 0, exporter);
    InstrumentedServiceImpl instrumentedService2 = new InstrumentedServiceImpl("id2", 0, exporter);
    InstrumentedServiceImpl instrumentedService3 = new InstrumentedServiceImpl("id3", 0, exporter);

    try (CallContext callContext1 = instrumentedService1.call(null)) {
      try (CallContext callContext2 = instrumentedService2.call(callContext1)) {
        /* empty by purpose */
      }
      try (CallContext callContext3 = instrumentedService3.call(callContext1)) {
        /* empty by purpose */
      }
    }

    List<SpanData> spans = exporter.getSpans();
    assertEquals(3, spans.size());

    SpanData span2 = spans.get(0); // span of second call is finished first
    SpanData span3 = spans.get(1);
    SpanData span1 = spans.get(2);

    assertEquals(span1.getTraceId(), span2.getTraceId());
    assertEquals(span1.getTraceId(), span3.getTraceId());
    assertEquals("0000000000000000", span1.getParentSpanId());
    assertEquals(span1.getSpanId(), span2.getParentSpanId());
    assertEquals(span1.getSpanId(), span3.getParentSpanId());
  }
}
