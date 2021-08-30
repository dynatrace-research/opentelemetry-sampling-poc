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

import com.dynatrace.research.otelsampling.exporter.CollectingSpanExporter;
import com.dynatrace.research.otelsampling.simulation.TraceUtil;
import com.dynatrace.research.otelsampling.tree.Tree;
import com.dynatrace.research.otelsampling.tree.TreeStructure;
import com.dynatrace.research.otelsampling.tree.TreeUtil;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.util.List;
import java.util.function.Function;
import org.junit.Test;

public class Scenario2Test {

  private static final Function<SpanData, String> SPAN_DATA_TO_STRING_MAPPER =
      s -> (s != null) ? (s.getSpanId() + " " + s.getName()) : "?";

  @Test
  public void testSamplingParentLinkMode() {

    int numNodes = 20;

    TreeStructure treeStructure = TreeUtil.createBalancedBinaryTree(numNodes);
    Tree<Integer> tree = new Tree<>(treeStructure, i -> i);

    CollectingSpanExporter collector = new CollectingSpanExporter();

    TraceUtil.simulate(
        tree,
        i ->
            new ConsistentFixedRateSampler((i == 0 || i == 3) ? 0. : 1.) {
              @Override
              protected RecordingMode getRecordingMode() {
                return RecordingMode.PARENT_LINK;
              }
            },
        Object::toString,
        collector,
        0);

    List<Tree<SpanData>> trees = TraceUtil.extractTrees(collector.getSpans());

    assertEquals(4, trees.size());

    String expected0 =
        "\n"
            + "482ee1d25a6947d7 span@8\n"
            + "|---950cdeabf1006da2 span@18\n"
            + "'---6b34073d1301e31e span@17";
    String expected1 =
        "\n"
            + "a07d5834d59b9a32 span@7\n"
            + "|---074e22e025c542a2 span@16\n"
            + "'---28a82d1d24b1d7ff span@15";
    String expected2 =
        "\n"
            + "9908e9ccec7d3723 span@2\n"
            + "|---ef378a8550a7720a span@6\n"
            + "|   |---e3fd83da40ab4c7f span@14\n"
            + "|   '---99308b6186f2d0c1 span@13\n"
            + "'---c5fa08bd46d5e088 span@5\n"
            + "    |---39c7a1aa48b5a7f0 span@12\n"
            + "    '---bdbc1babc3bcb4e6 span@11";
    String expected3 =
        "\n"
            + "42795bcf4c0c701a span@1\n"
            + "'---75fe54f3f1c96514 span@4\n"
            + "    |---15d635f0effb2ca0 span@10\n"
            + "    '---c196d4a52e3ea1c2 span@9\n"
            + "        '---5167a27d0828837e span@19";
    assertEquals(expected0, TreeUtil.printTree(trees.get(0), SPAN_DATA_TO_STRING_MAPPER, 4, "\n"));
    assertEquals(expected1, TreeUtil.printTree(trees.get(1), SPAN_DATA_TO_STRING_MAPPER, 4, "\n"));
    assertEquals(expected2, TreeUtil.printTree(trees.get(2), SPAN_DATA_TO_STRING_MAPPER, 4, "\n"));
    assertEquals(expected3, TreeUtil.printTree(trees.get(3), SPAN_DATA_TO_STRING_MAPPER, 4, "\n"));
  }

  @Test
  public void testSamplingAncestorLinkMode() {

    int numNodes = 20;

    TreeStructure treeStructure = TreeUtil.createBalancedBinaryTree(numNodes);
    Tree<Integer> tree = new Tree<>(treeStructure, i -> i);

    CollectingSpanExporter collector = new CollectingSpanExporter();

    TraceUtil.simulate(
        tree,
        i ->
            new ConsistentFixedRateSampler((i == 0 || i == 3) ? 0. : 1.) {
              @Override
              protected RecordingMode getRecordingMode() {
                return RecordingMode.ANCESTOR_LINK;
              }
            },
        Object::toString,
        collector,
        0);

    List<Tree<SpanData>> trees = TraceUtil.extractTrees(collector.getSpans());

    assertEquals(2, trees.size());

    String expected0 =
        "\n"
            + "9908e9ccec7d3723 span@2\n"
            + "|---ef378a8550a7720a span@6\n"
            + "|   |---e3fd83da40ab4c7f span@14\n"
            + "|   '---99308b6186f2d0c1 span@13\n"
            + "'---c5fa08bd46d5e088 span@5\n"
            + "    |---39c7a1aa48b5a7f0 span@12\n"
            + "    '---bdbc1babc3bcb4e6 span@11";
    String expected1 =
        "\n"
            + "42795bcf4c0c701a span@1\n"
            + "|---482ee1d25a6947d7 span@8\n"
            + "|   |---950cdeabf1006da2 span@18\n"
            + "|   '---6b34073d1301e31e span@17\n"
            + "|---a07d5834d59b9a32 span@7\n"
            + "|   |---074e22e025c542a2 span@16\n"
            + "|   '---28a82d1d24b1d7ff span@15\n"
            + "'---75fe54f3f1c96514 span@4\n"
            + "    |---15d635f0effb2ca0 span@10\n"
            + "    '---c196d4a52e3ea1c2 span@9\n"
            + "        '---5167a27d0828837e span@19";

    assertEquals(expected0, TreeUtil.printTree(trees.get(0), SPAN_DATA_TO_STRING_MAPPER, 4, "\n"));
    assertEquals(expected1, TreeUtil.printTree(trees.get(1), SPAN_DATA_TO_STRING_MAPPER, 4, "\n"));
  }

  @Test
  public void testSamplingAncestorLinkAndDistanceMode() {

    int numNodes = 20;

    TreeStructure treeStructure = TreeUtil.createBalancedBinaryTree(numNodes);
    Tree<Integer> tree = new Tree<>(treeStructure, i -> i);

    CollectingSpanExporter collector = new CollectingSpanExporter();

    TraceUtil.simulate(
        tree,
        i ->
            new ConsistentFixedRateSampler((i == 0 || i == 3) ? 0. : 1.) {
              @Override
              protected RecordingMode getRecordingMode() {
                return RecordingMode.ANCESTOR_LINK_AND_DISTANCE;
              }
            },
        Object::toString,
        collector,
        0);

    List<Tree<SpanData>> trees = TraceUtil.extractTrees(collector.getSpans());

    assertEquals(2, trees.size());

    String expected0 =
        "\n"
            + "9908e9ccec7d3723 span@2\n"
            + "|---ef378a8550a7720a span@6\n"
            + "|   |---e3fd83da40ab4c7f span@14\n"
            + "|   '---99308b6186f2d0c1 span@13\n"
            + "'---c5fa08bd46d5e088 span@5\n"
            + "    |---39c7a1aa48b5a7f0 span@12\n"
            + "    '---bdbc1babc3bcb4e6 span@11";
    String expected1 =
        "\n"
            + "42795bcf4c0c701a span@1\n"
            + "|---?\n"
            + "|   '---482ee1d25a6947d7 span@8\n"
            + "|       |---950cdeabf1006da2 span@18\n"
            + "|       '---6b34073d1301e31e span@17\n"
            + "|---?\n"
            + "|   '---a07d5834d59b9a32 span@7\n"
            + "|       |---074e22e025c542a2 span@16\n"
            + "|       '---28a82d1d24b1d7ff span@15\n"
            + "'---75fe54f3f1c96514 span@4\n"
            + "    |---15d635f0effb2ca0 span@10\n"
            + "    '---c196d4a52e3ea1c2 span@9\n"
            + "        '---5167a27d0828837e span@19";

    assertEquals(expected0, TreeUtil.printTree(trees.get(0), SPAN_DATA_TO_STRING_MAPPER, 4, "\n"));
    assertEquals(expected1, TreeUtil.printTree(trees.get(1), SPAN_DATA_TO_STRING_MAPPER, 4, "\n"));
  }
}
