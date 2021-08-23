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

import io.opentelemetry.api.trace.SpanId;
import io.opentelemetry.api.trace.TraceId;
import io.opentelemetry.sdk.trace.IdGenerator;
import java.util.SplittableRandom;

public class DeterministicIdGenerator implements IdGenerator {

  private static final long INVALID_ID = 0;
  private final SplittableRandom random;

  public DeterministicIdGenerator(long seed) {
    random = new SplittableRandom(seed);
  }

  @Override
  public String generateSpanId() {
    long id;
    do {
      id = random.nextLong();
    } while (id == INVALID_ID);
    return SpanId.fromLong(id);
  }

  @Override
  public String generateTraceId() {
    long idHi;
    long idLo;
    do {
      idHi = random.nextLong();
      idLo = random.nextLong();
    } while (idHi == INVALID_ID && idLo == INVALID_ID);
    return TraceId.fromLongs(idHi, idLo);
  }
}
