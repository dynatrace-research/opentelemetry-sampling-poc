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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

import java.util.*;
import java.util.function.IntToDoubleFunction;
import java.util.function.ToIntFunction;

public class ReservoirSampler<T> {

  public static final class Sample<T> {
    private final T item;
    private final int sampleRateIndex;

    public Sample(T item, int sampleRateIndex) {
      this.item = item;
      this.sampleRateIndex = sampleRateIndex;
    }

    public T getItem() {
      return item;
    }

    public int getSampleRateIndex() {
      return sampleRateIndex;
    }

    @Override
    public String toString() {
      return "Sample{" + "item=" + item + ", sampleRateIndex=" + sampleRateIndex + '}';
    }
  }

  private final ToIntFunction<? super T> greatestSampleRateIndexGreaterThanRandomValueOfData;
  private final IntToDoubleFunction sampleRateIndexToSampleRate;

  private final Object[] buffer;
  private int counter = 0;
  private int bufferSeparatorIndex = 0;
  private int bufferSampleRateIndex = 0;
  private final SplittableRandom random;

  // elements with indices < bufferSeparatorIndex are sampled with a rate defined by
  // bufferSampleRateIndex
  // elements with indices >= bufferSeparatorIndex are sampled with a rate defined by
  // bufferSampleRateIndex - 1
  // bufferSeparatorIndex < buffer.length must always hold
  // if bufferSampleRateIndex == 0, the buffer is not yet full, and contains bufferSeparatorIndex
  // elements
  void checkConsistency() {
    checkState(bufferSeparatorIndex < buffer.length);
    for (int i = 0; i < bufferSeparatorIndex; ++i) {
      checkState(
          greatestSampleRateIndexGreaterThanRandomValueOfData.applyAsInt((T) buffer[i])
              >= bufferSampleRateIndex);
      checkState(buffer[i] != null);
    }
    if (bufferSampleRateIndex > 0) {
      for (int i = bufferSeparatorIndex; i < buffer.length; ++i) {
        checkState(
            greatestSampleRateIndexGreaterThanRandomValueOfData.applyAsInt((T) buffer[i])
                >= bufferSampleRateIndex - 1);
        checkState(buffer[i] != null);
      }
    } else {
      for (int i = bufferSeparatorIndex; i < buffer.length; ++i) {
        checkState(buffer[i] == null);
      }
    }
  }

  public ReservoirSampler(
      int capacity,
      ToIntFunction<? super T> greatestSampleRateIndexGreaterThanRandomValueOfData,
      IntToDoubleFunction sampleRateIndexToSampleRate,
      SplittableRandom random) {
    checkArgument(capacity > 0);
    buffer = new Object[capacity];
    this.greatestSampleRateIndexGreaterThanRandomValueOfData =
        requireNonNull(greatestSampleRateIndexGreaterThanRandomValueOfData);
    this.sampleRateIndexToSampleRate = requireNonNull(sampleRateIndexToSampleRate);
    checkArgument(sampleRateIndexToSampleRate.applyAsDouble(0) == 1.);
    this.random = requireNonNull(random);
    checkConsistency();
  }

  public void add(T item) {
    requireNonNull(item);
    if (bufferSampleRateIndex == 0) {
      // buffer is not yet full
      buffer[bufferSeparatorIndex] = item;
      bufferSeparatorIndex += 1;
      if (bufferSeparatorIndex == buffer.length) {
        bufferSeparatorIndex = 0;
        bufferSampleRateIndex += 1;
        counter = buffer.length;
      }
    } else {
      // buffer is full
      while (greatestSampleRateIndexGreaterThanRandomValueOfData.applyAsInt(item)
          >= bufferSampleRateIndex - 1) {
        counter += 1;
        int idxToDrop = random.nextInt(bufferSeparatorIndex, bufferSeparatorIndex + counter);
        if (idxToDrop < buffer.length) {
          T tmp = (T) buffer[idxToDrop];
          buffer[idxToDrop] = item;
          item = tmp;
        }
        while (bufferSeparatorIndex < buffer.length
            && greatestSampleRateIndexGreaterThanRandomValueOfData.applyAsInt(item)
                >= bufferSampleRateIndex) {
          int idx = random.nextInt(bufferSeparatorIndex, buffer.length);
          final T tmp = (T) buffer[idx];
          buffer[idx] = buffer[bufferSeparatorIndex];
          buffer[bufferSeparatorIndex] = item;
          item = tmp;
          bufferSeparatorIndex += 1;
        }
        if (bufferSeparatorIndex == buffer.length) {
          bufferSeparatorIndex = 0;
          bufferSampleRateIndex += 1;
          counter = buffer.length;
        } else {
          break;
        }
      }
    }
    // checkConsistency();
  }

  public Collection<Sample<T>> getSamples() {
    List<Sample<T>> list =
        new ArrayList<>((bufferSampleRateIndex > 0) ? buffer.length : bufferSeparatorIndex);
    for (int i = 0; i < bufferSeparatorIndex; ++i) {
      list.add(new Sample<>((T) buffer[i], bufferSampleRateIndex));
    }
    if (bufferSampleRateIndex > 0) {
      for (int i = bufferSeparatorIndex; i < buffer.length; ++i) {
        list.add(new Sample<>((T) buffer[i], bufferSampleRateIndex - 1));
      }
    }

    return list;
  }
}
