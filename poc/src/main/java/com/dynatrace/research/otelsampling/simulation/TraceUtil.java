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

import static java.util.stream.Collectors.groupingBy;

import com.dynatrace.research.otelsampling.sampling.SamplingUtil;
import com.dynatrace.research.otelsampling.simulation.InstrumentedService.CallContext;
import com.dynatrace.research.otelsampling.tree.Tree;
import com.dynatrace.research.otelsampling.tree.TreeStructure;
import com.dynatrace.research.otelsampling.tree.TreeStructure.Builder;
import com.google.common.base.Preconditions;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import java.util.*;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class TraceUtil {

  private TraceUtil() {}

  public static <ID_TYPE> void simulate(
      Tree<ID_TYPE> tree,
      Function<? super ID_TYPE, ? extends Sampler> samplerProvider,
      Function<? super ID_TYPE, String> idToStringConverter,
      SpanExporter spanExporter,
      long hashSalt) {

    int numNodes = tree.getTreeStructure().getNumberOfNodes();
    List<CallContext> callContexts = new ArrayList<>(numNodes);

    for (int nodeIdx = 0; nodeIdx < numNodes; ++nodeIdx) {
      ID_TYPE id = tree.get(nodeIdx);
      InstrumentedService instrumentedService =
          new InstrumentedServiceImpl(
              idToStringConverter.apply(tree.get(nodeIdx)), hashSalt, spanExporter);
      instrumentedService.setSampler(samplerProvider.apply(id));
      int parentId = tree.getTreeStructure().getParentId(nodeIdx);
      CallContext parentCallText =
          (parentId != TreeStructure.NO_PARENT_ID) ? callContexts.get(parentId) : null;
      callContexts.add(instrumentedService.call(parentCallText));
    }

    for (int i = numNodes - 1; i >= 0; --i) {
      callContexts.get(i).close();
    }
  }

  // TODO in some cases subtrees could be merged, by adding an unknown common root node
  private static List<Tree<SpanData>> extractTreesHelper(
      Collection<? extends SpanData> spanDataCollection) {

    Preconditions.checkArgument(
        spanDataCollection.stream().map(SpanData::getTraceId).distinct().count() == 1);

    Map<String, SpanData> index =
        spanDataCollection.stream()
            .collect(Collectors.toMap(SpanData::getSpanId, Function.identity()));
    Map<String, List<SpanData>> childSpans =
        spanDataCollection.stream()
            .collect(
                Collectors.groupingBy(
                    SamplingUtil::getParentSpanId, Collectors.<SpanData>toList()));
    Collection<SpanData> rootSpans =
        spanDataCollection.stream()
            .filter(s -> !index.containsKey(SamplingUtil.getParentSpanId(s)))
            .collect(Collectors.<SpanData>toList());

    List<Tree<SpanData>> result = new ArrayList<>(rootSpans.size());

    for (SpanData rootSpan : rootSpans) {
      Map<String, Integer> spanToIndex = new HashMap<>();
      spanToIndex.put(rootSpan.getSpanId(), 0);
      Builder builder = TreeStructure.builder();
      int spanCounter = 1;

      Deque<SpanData> buffer =
          new ArrayDeque<>(childSpans.getOrDefault(rootSpan.getSpanId(), Collections.emptyList()));
      while (!buffer.isEmpty()) {
        SpanData s = buffer.remove();
        builder.addNode(spanToIndex.get(SamplingUtil.getParentSpanId(s)));
        for (int i = 0; i < SamplingUtil.getParentDistance(s); ++i) {
          builder.addNode(spanCounter);
          spanCounter += 1;
        }
        spanToIndex.put(s.getSpanId(), spanCounter);
        spanCounter += 1;

        buffer.addAll(childSpans.getOrDefault(s.getSpanId(), Collections.emptyList()));
      }

      TreeStructure treeStructure = builder.build();
      Tree<SpanData> tree = new Tree<>(treeStructure);

      for (Entry<String, Integer> entry : spanToIndex.entrySet()) {
        tree.set(entry.getValue(), index.get(entry.getKey()));
      }

      result.add(tree);
    }
    return result;
  }

  public static List<Tree<SpanData>> extractTrees(
      Collection<? extends SpanData> spanDataCollection) {

    Map<String, List<SpanData>> spanDataGroupedByTraceId =
        spanDataCollection.stream().collect(groupingBy(SpanData::getTraceId, Collectors.toList()));

    return spanDataGroupedByTraceId.values().stream()
        .flatMap(x -> extractTreesHelper(x).stream())
        .collect(Collectors.toList());
  }

  /*private static double[] extractAllSampleRates(Tree<SpanData> tree) {
    Set<Double> sampleRates = new HashSet<>();
    for (int nodeIdx = 0; nodeIdx < tree.getTreeStructure().getNumberOfNodes(); ++nodeIdx) {
      SpanData spanData = tree.get(nodeIdx);
      if (spanData != null) {
        sampleRates.add(SamplingUtil.getSamplingRatio(tree.get(nodeIdx)));
      }
    }
    sampleRates.add(1.);
    return sampleRates.stream().mapToDouble(Double::doubleValue).sorted().toArray();
  }*/
}
