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
import com.dynatrace.research.otelsampling.tree.Tree;
import com.dynatrace.research.otelsampling.tree.TreeStructure;
import com.dynatrace.research.otelsampling.tree.TreeUtil;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import java.util.List;
import org.junit.Test;

public class TraceUtilTest {

  @Test
  public void test1() {

    int numNodes = 10;

    TreeStructure treeStructure = TreeUtil.createBalancedBinaryTree(numNodes);
    Tree<Integer> tree = new Tree<>(treeStructure, i -> i);

    CollectingSpanExporter collector = new CollectingSpanExporter();

    TraceUtil.simulate(tree, i -> Sampler.traceIdRatioBased(1), Object::toString, collector, 0);

    List<Tree<SpanData>> trees = TraceUtil.extractTrees(collector.getSpans());

    assertEquals(1, trees.size());

    String expected0 =
        "\n"
            + "c2f5d114b285a33d span@0\n"
            + "|---9908e9ccec7d3723 span@2\n"
            + "|   |---ef378a8550a7720a span@6\n"
            + "|   '---c5fa08bd46d5e088 span@5\n"
            + "'---42795bcf4c0c701a span@1\n"
            + "    |---75fe54f3f1c96514 span@4\n"
            + "    |   '---c196d4a52e3ea1c2 span@9\n"
            + "    '---aa589b8a2fd87442 span@3\n"
            + "        |---482ee1d25a6947d7 span@8\n"
            + "        '---a07d5834d59b9a32 span@7";
    assertEquals(
        expected0,
        TreeUtil.printTree(trees.get(0), s -> s.getSpanId() + " " + s.getName(), 4, "\n"));
  }

  @Test
  public void test2() {

    int numNodes = 10;

    TreeStructure treeStructure = TreeUtil.createBalancedBinaryTree(numNodes);
    Tree<Integer> tree = new Tree<>(treeStructure, i -> i);

    CollectingSpanExporter collector = new CollectingSpanExporter();

    TraceUtil.simulate(
        tree, i -> Sampler.traceIdRatioBased((i == 1) ? 0 : 1), Object::toString, collector, 0);

    List<Tree<SpanData>> trees = TraceUtil.extractTrees(collector.getSpans());

    assertEquals(3, trees.size());
    String expected0 = "\n" + "75fe54f3f1c96514\n" + "'---c196d4a52e3ea1c2";
    String expected1 =
        "\n" + "aa589b8a2fd87442\n" + "|---482ee1d25a6947d7\n" + "'---a07d5834d59b9a32";
    String expected2 =
        "\n"
            + "c2f5d114b285a33d\n"
            + "'---9908e9ccec7d3723\n"
            + "    |---ef378a8550a7720a\n"
            + "    '---c5fa08bd46d5e088";
    assertEquals(expected0, TreeUtil.printTree(trees.get(0), SpanData::getSpanId, 4, "\n"));
    assertEquals(expected1, TreeUtil.printTree(trees.get(1), SpanData::getSpanId, 4, "\n"));
    assertEquals(expected2, TreeUtil.printTree(trees.get(2), SpanData::getSpanId, 4, "\n"));
  }
}
