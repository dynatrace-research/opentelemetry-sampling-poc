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
package com.dynatrace.research.otelsampling.tree;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkElementIndex;

import com.dynatrace.research.otelsampling.tree.TreeStructure.Builder;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;
import java.util.function.Function;
import java.util.function.IntConsumer;
import java.util.function.IntFunction;

public final class TreeUtil {

  private static final String DEFAULT_LINE_FEED = "\n";
  private static final int DEFAULT_INDENT = 4;

  private TreeUtil() {}

  public static TreeStructure createBalancedBinaryTree(int numNodes) {
    return createBalancedTree(numNodes, 2);
  }

  public static TreeStructure createChain(int numNodes) {
    return createBalancedTree(numNodes, 1);
  }

  public static TreeStructure createBalancedTree(int numNodes, int numChildrenPerNode) {
    checkArgument(numNodes >= 0);
    checkArgument(numChildrenPerNode > 0);
    if (numNodes == 0) {
      return TreeStructure.empty();
    }
    Builder builder = TreeStructure.builder();
    for (int i = 0; i < numNodes - 1; ++i) {
      builder.addNode(i / numChildrenPerNode);
    }
    return builder.build();
  }

  public static TreeStructure generateRandomTree(long seed, int numNodes) {
    SplittableRandom random = new SplittableRandom(seed);
    checkArgument(numNodes >= 0);
    if (numNodes == 0) {
      return TreeStructure.empty();
    }
    Builder builder = TreeStructure.builder();
    for (int i = 1; i < numNodes; ++i) {
      builder.addNode(random.nextInt(i));
    }
    return builder.build();
  }

  private static void getDepthFirstOrderHelper(
      TreeStructure treeStructure, int nodeIndex, IntConsumer nodeIndexConsumer) {
    nodeIndexConsumer.accept(nodeIndex);
    for (int childIndex : treeStructure.getChildrenIds(nodeIndex)) {
      getDepthFirstOrderHelper(treeStructure, childIndex, nodeIndexConsumer);
    }
  }

  public static void iterateDepthFirstOrder(
      TreeStructure treeStructure, IntConsumer nodeIndexConsumer) {
    if (treeStructure.getNumberOfNodes() > 0) {
      getDepthFirstOrderHelper(treeStructure, TreeStructure.ROOT_ID, nodeIndexConsumer);
    }
  }

  public static int getLevel(TreeStructure treeStructure, int nodeIndex) {
    checkElementIndex(nodeIndex, treeStructure.getNumberOfNodes());
    int level = 0;
    while (nodeIndex != TreeStructure.ROOT_ID) {
      level += 1;
      nodeIndex = treeStructure.getParentId(nodeIndex);
    }
    return level;
  }

  public static String printStructure(
      TreeStructure treeStructure, IntFunction<String> labels, int indent, String lineFeed) {
    Preconditions.checkArgument(indent > 0);
    StringBuilder sb = new StringBuilder();
    iterateDepthFirstOrder(
        treeStructure,
        nodeIndex -> {
          sb.append(lineFeed);
          List<Boolean> lastChildIndicators = new ArrayList<>();
          int n = nodeIndex;
          while (n != TreeStructure.ROOT_ID) {
            int parentNodeIndex = treeStructure.getParentId(n);
            int[] siblingNodeIndices = treeStructure.getChildrenIds(parentNodeIndex);
            lastChildIndicators.add(siblingNodeIndices[siblingNodeIndices.length - 1] == n);
            n = parentNodeIndex;
          }

          for (int i = lastChildIndicators.size() - 1; i > 0; --i) {
            if (lastChildIndicators.get(i)) {
              sb.append(' ');
            } else {
              sb.append('|');
            }
            for (int j = 0; j < indent - 1; j++) {
              sb.append(' ');
            }
          }
          if (lastChildIndicators.size() > 0) {
            if (lastChildIndicators.get(0)) {
              sb.append('\'');
            } else {
              sb.append('|');
            }
            for (int j = 0; j < indent - 1; j++) {
              sb.append('-');
            }
          }
          sb.append(labels.apply(nodeIndex));
        });

    return sb.toString();
  }

  public static String printStructure(
      TreeStructure treeStructure, IntFunction<String> labels, int indent) {
    return printStructure(treeStructure, labels, indent, DEFAULT_LINE_FEED);
  }

  public static String printStructure(TreeStructure treeStructure, IntFunction<String> labels) {
    return printStructure(treeStructure, labels, DEFAULT_INDENT, DEFAULT_LINE_FEED);
  }

  public static <V> String printTree(
      Tree<V> tree, Function<? super V, String> stringMapper, int indent, String lineFeed) {
    return printStructure(
        tree.getTreeStructure(), i -> stringMapper.apply(tree.get(i)), indent, lineFeed);
  }

  public static <V> String printTree(
      Tree<V> tree, Function<? super V, String> stringMapper, int indent) {
    return printTree(tree, stringMapper, indent, DEFAULT_LINE_FEED);
  }

  public static <V> String printTree(Tree<V> tree, Function<? super V, String> stringMapper) {
    return printTree(tree, stringMapper, DEFAULT_INDENT, DEFAULT_LINE_FEED);
  }

  public static <V> String printTree(Tree<V> tree) {
    return printTree(tree, Object::toString, DEFAULT_INDENT, DEFAULT_LINE_FEED);
  }
}
