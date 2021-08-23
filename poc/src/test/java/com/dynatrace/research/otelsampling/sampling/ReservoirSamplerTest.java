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

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertFalse;

import java.util.*;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.hipparchus.stat.descriptive.StreamingStatistics;
import org.hipparchus.stat.inference.TTest;
import org.junit.Test;

public class ReservoirSamplerTest {

  // this function assumes that the argument is a random long
  // this long can be interpreted as random number in [0,1)
  // if the allowed sample rates are powers of two: 1, 1/2, 1/4, 1/8,...,
  // the greatest sample rate index whose sample rate is greater than this random number
  // is simply the number of leading zeros of the argument
  private static int greatestSampleRateIndexGreaterThanRandomValueOfData(long l) {
    return Long.numberOfLeadingZeros(l);
  }

  // this function describes the allowed sample rates
  // it is a descending sequence of sample rates
  // this function defines the sample rates as powers of two 1, 1/2, 1/4, 1/8,...
  private static double sampleRateIndexToSampleRate(int i) {
    return 1. / sampleRateIndexToExtrapolationFactor(i);
  }

  private static long sampleRateIndexToExtrapolationFactor(int i) {
    return (1L << i);
  }

  @Test
  public void testConsistency() {
    SplittableRandom randomSampler = new SplittableRandom(0);
    SplittableRandom randomGenerator = new SplittableRandom(234);
    int capacity = 20;

    ReservoirSampler<Long> sampler =
        new ReservoirSampler<>(
            capacity,
            ReservoirSamplerTest::greatestSampleRateIndexGreaterThanRandomValueOfData,
            ReservoirSamplerTest::sampleRateIndexToSampleRate,
            randomSampler);
    sampler.checkConsistency();
    for (int i = 0; i < 1000; ++i) {
      sampler.add(randomGenerator.nextLong());
      sampler.checkConsistency();
    }
  }

  private static final class Item {
    int maxSampleRateIndex;
    int idx;

    public Item(int idx, int maxSampleRateIndex) {
      this.maxSampleRateIndex = maxSampleRateIndex;
      this.idx = idx;
    }

    public int getMaxSampleRateIndex() {
      return maxSampleRateIndex;
    }

    public int getIdx() {
      return idx;
    }
  }

  @Test
  public void testBalancedSampling() {
    SplittableRandom randomGenerator = new SplittableRandom(0x88e23b990958cdcdL);

    double alpha = 0.01;
    int capacity = 10;
    int numItems = 100;
    long numIterations = 10000000;

    List<StreamingStatistics> statistics =
        Stream.generate(StreamingStatistics::new).limit(numItems).collect(toList());

    for (long i = 0; i < numIterations; ++i) {
      int[] maxSampleRateIndices =
          LongStream.generate(randomGenerator::nextLong)
              .limit(numItems)
              .mapToInt(Long::numberOfLeadingZeros)
              .toArray();

      List<Item> items =
          IntStream.range(0, numItems)
              .mapToObj(k -> new Item(k, maxSampleRateIndices[k]))
              .collect(toList());
      ReservoirSampler<Item> reservoirSampler =
          new ReservoirSampler<>(
              capacity,
              Item::getMaxSampleRateIndex,
              ReservoirSamplerTest::sampleRateIndexToSampleRate,
              randomGenerator);
      items.forEach(reservoirSampler::add);

      for (ReservoirSampler.Sample<Item> sample : reservoirSampler.getSamples()) {
        Item item = sample.getItem();
        int sampleRateIndex = sample.getSampleRateIndex();
        statistics.get(item.getIdx()).accept(sampleRateIndexToExtrapolationFactor(sampleRateIndex));
      }
    }

    double expectedMean = (double) numItems / (double) capacity;
    double individualAlpha =
        -Math.expm1(
            Math.log1p(-alpha)
                / numItems); // calculate alpha for individual t-test, such that the probability
    // that there is one false positive out of numItems t-tests is equal to
    // alpha

    assertFalse(
        statistics.stream().anyMatch(s -> new TTest().tTest(expectedMean, s, individualAlpha)));
  }
}
