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

import com.dynatrace.research.otelsampling.exporter.CollectingSpanExporter;
import com.dynatrace.research.otelsampling.simulation.TraceUtil;
import com.dynatrace.research.otelsampling.tree.Tree;
import com.dynatrace.research.otelsampling.tree.TreeStructure;
import com.dynatrace.research.otelsampling.tree.TreeUtil;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import java.util.stream.DoubleStream;
import org.hipparchus.stat.inference.AlternativeHypothesis;
import org.hipparchus.stat.inference.BinomialTest;
import org.hipparchus.stat.inference.GTest;
import org.junit.Test;

public class SamplingTest {

  @Test
  public void testSimulate() {

    CollectingSpanExporter collector = new CollectingSpanExporter();

    TreeStructure treeStructure = TreeUtil.createBalancedBinaryTree(20);
    Tree<Integer> tree = new Tree<>(treeStructure, i -> i);

    TraceUtil.simulate(tree, i -> Sampler.alwaysOn(), Object::toString, collector, 0L);

    assertEquals(20, collector.getSpans().size());
  }

  @Test
  public void testIdRatioBasedSamplerHomogeneousSamplingRate() {

    double alpha = 0.01;

    int numCycles = 10000;
    int numNodes = 20;

    double sampleRate = 0.5;

    int sampledAllCounter = 0;

    TreeStructure treeStructure = TreeUtil.createBalancedBinaryTree(numNodes);
    Tree<Integer> tree = new Tree<>(treeStructure, i -> i);

    for (int cycleIdx = 0; cycleIdx < numCycles; ++cycleIdx) {

      CollectingSpanExporter collector = new CollectingSpanExporter();

      TraceUtil.simulate(
          tree, i -> Sampler.traceIdRatioBased(sampleRate), Object::toString, collector, cycleIdx);

      int numberOfSampledSpans = collector.getSpans().size();

      if (numberOfSampledSpans == numNodes) {
        sampledAllCounter += 1;
      } else {
        assertEquals(0, numberOfSampledSpans);
      }
    }

    assertFalse(
        new BinomialTest()
            .binomialTest(
                numCycles, sampledAllCounter, sampleRate, AlternativeHypothesis.TWO_SIDED, alpha));
  }

  @Test
  public void testIdRatioBasedSamplerInhomogeneousSamplingRates() {

    int numCycles = 10000;

    int numNodes = 3;
    double alpha = 0.01;

    TreeStructure treeStructure = TreeUtil.createChain(numNodes);
    Tree<Integer> tree = new Tree<>(treeStructure, i -> i);

    double[] sampleRates = {0.5, 0.3, 0.7};
    assertEquals(numNodes, sampleRates.length);

    long[] histogram = new long[numNodes + 1];
    for (int cycleIdx = 0; cycleIdx < numCycles; ++cycleIdx) {

      CollectingSpanExporter collector = new CollectingSpanExporter();

      TraceUtil.simulate(
          tree,
          i -> Sampler.traceIdRatioBased(sampleRates[i]),
          Object::toString,
          collector,
          cycleIdx);

      int numberOfSampledSpans = collector.getSpans().size();

      histogram[numberOfSampledSpans] += 1;
    }
    double[] sortedSampleRates =
        DoubleStream.concat(DoubleStream.of(0, 1), DoubleStream.of(sampleRates)).sorted().toArray();

    double[] expectedFrequencies = new double[numNodes + 1];
    expectedFrequencies[0] = 1. - sortedSampleRates[numNodes];
    for (int i = 0; i <= numNodes; ++i) {
      expectedFrequencies[i] =
          sortedSampleRates[numNodes + 1 - i] - sortedSampleRates[numNodes - i];
    }

    assertFalse(new GTest().gTest(expectedFrequencies, histogram, alpha));
  }
}
