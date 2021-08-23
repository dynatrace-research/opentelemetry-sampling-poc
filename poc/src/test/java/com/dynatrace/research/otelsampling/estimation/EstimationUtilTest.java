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
package com.dynatrace.research.otelsampling.estimation;

import static com.dynatrace.research.otelsampling.estimation.ScalarQuantityExtractor.countMatchingSpans;
import static java.util.stream.Collectors.toMap;
import static org.junit.Assert.assertFalse;

import com.dynatrace.research.otelsampling.exporter.CollectingSpanExporter;
import com.dynatrace.research.otelsampling.sampling.AdvancedTraceIdRatioBasedSampler;
import com.dynatrace.research.otelsampling.sampling.SamplingMode;
import com.dynatrace.research.otelsampling.simulation.TraceUtil;
import com.dynatrace.research.otelsampling.tree.Tree;
import com.dynatrace.research.otelsampling.tree.TreeUtil;
import com.google.common.collect.ImmutableSet;
import java.util.*;
import java.util.function.Function;
import java.util.stream.DoubleStream;
import org.hipparchus.stat.descriptive.StreamingStatistics;
import org.hipparchus.stat.inference.TTest;
import org.junit.Test;

public class EstimationUtilTest {

  @Test
  public void testExtrapolationOfSpanCounts() {

    double alpha = 0.01;

    int numNodes = 20;
    Tree<Integer> treeTemplate = new Tree<>(TreeUtil.createBalancedBinaryTree(numNodes), i -> i);
    int numCallTrees = 100000;

    SplittableRandom random = new SplittableRandom(1L);

    Map<String, StreamingStatistics> groupedObservedStats = new HashMap<>();
    for (int k = 0; k < numNodes; ++k) {
      groupedObservedStats.put("span@" + k, new StreamingStatistics());
    }

    Map<String, ScalarQuantityExtractor> extractors =
        groupedObservedStats.keySet().stream()
            .collect(
                toMap(
                    Function.identity(),
                    name -> countMatchingSpans(s -> name.equals(s.getName()))));

    VectorQuantityExtractor<String> extractor = VectorQuantityExtractor.of(extractors);

    for (int callTreeIdx = 0; callTreeIdx < numCallTrees; ++callTreeIdx) {
      long hashSalt = random.nextLong();
      double[] samplingRates =
          DoubleStream.generate(random::nextDouble)
              .limit(treeTemplate.getTreeStructure().getNumberOfNodes())
              .toArray();

      CollectingSpanExporter spanExporter = new CollectingSpanExporter();
      TraceUtil.simulate(
          treeTemplate,
          i -> AdvancedTraceIdRatioBasedSampler.create(SamplingMode.PARENT_LINK, samplingRates[i]),
          Object::toString,
          spanExporter,
          hashSalt);

      EstimationUtil.estimate(extractor, spanExporter.getSpans())
          .forEach((key, quantity) -> groupedObservedStats.get(key).accept(quantity));
    }

    for (StreamingStatistics observedStats : groupedObservedStats.values()) {
      assertFalse(
          new TTest().tTest(numCallTrees / (double) observedStats.getN(), observedStats, alpha));
    }
  }

  @Test
  public void testExtrapolationOfParentChildCalls() {

    double alpha = 0.01;

    int numNodes = 20;
    Tree<Integer> treeTemplate = new Tree<>(TreeUtil.createBalancedBinaryTree(numNodes), i -> i);
    int numCallTrees = 100000;

    SplittableRandom random = new SplittableRandom(1L);

    ParentChildRelationshipCounter parentChildRelationshipCounter =
        new ParentChildRelationshipCounter(
            s -> "span@1".equals(s.getName()),
            s -> ImmutableSet.of("span@15", "span@17", "span@18").contains(s.getName()));

    StreamingStatistics observedStats = new StreamingStatistics();

    for (int callTreeIdx = 0; callTreeIdx < numCallTrees; ++callTreeIdx) {
      long hashSalt = random.nextLong();
      double[] samplingRates =
          DoubleStream.generate(random::nextDouble)
              .limit(treeTemplate.getTreeStructure().getNumberOfNodes())
              .toArray();

      CollectingSpanExporter spanExporter = new CollectingSpanExporter();
      TraceUtil.simulate(
          treeTemplate,
          i -> AdvancedTraceIdRatioBasedSampler.create(SamplingMode.PARENT_LINK, samplingRates[i]),
          Object::toString,
          spanExporter,
          hashSalt);

      observedStats.accept(
          EstimationUtil.estimate(parentChildRelationshipCounter, spanExporter.getSpans()));
    }

    assertFalse(
        new TTest().tTest(3 * numCallTrees / (double) observedStats.getN(), observedStats, alpha));
  }

  @Test
  public void testExtrapolationOfFiveMinusNumberParentChildCalls() {

    double alpha = 0.01;

    int numNodes = 20;
    Tree<Integer> treeTemplate = new Tree<>(TreeUtil.createBalancedBinaryTree(numNodes), i -> i);
    int numCallTrees = 100000;

    SplittableRandom random = new SplittableRandom(2L);

    ParentChildRelationshipCounter parentChildRelationshipCounter =
        new ParentChildRelationshipCounter(
            s -> "span@1".equals(s.getName()),
            s -> ImmutableSet.of("span@15", "span@17", "span@18").contains(s.getName()));

    StreamingStatistics observedStats = new StreamingStatistics();

    for (int callTreeIdx = 0; callTreeIdx < numCallTrees; ++callTreeIdx) {
      long hashSalt = random.nextLong();
      double[] samplingRates =
          DoubleStream.generate(random::nextDouble)
              .limit(treeTemplate.getTreeStructure().getNumberOfNodes())
              .toArray();

      CollectingSpanExporter spanExporter = new CollectingSpanExporter();
      TraceUtil.simulate(
          treeTemplate,
          i -> AdvancedTraceIdRatioBasedSampler.create(SamplingMode.PARENT_LINK, samplingRates[i]),
          Object::toString,
          spanExporter,
          hashSalt);

      observedStats.accept(
          EstimationUtil.estimate(
              x -> 5. - parentChildRelationshipCounter.extract(x), spanExporter.getSpans()));
    }

    assertFalse(
        new TTest()
            .tTest((5. - 3) * numCallTrees / (double) observedStats.getN(), observedStats, alpha));
  }
}
