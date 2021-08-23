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

import com.dynatrace.research.otelsampling.tree.TreeStructure.Builder;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

final class TreeStructureBuilder implements TreeStructure.Builder {
  final List<Integer> tmpParentIndices;

  @Override
  public TreeStructure build() {
    return new TreeStructure() {

      private final int[] parentIndices = tmpParentIndices.stream().mapToInt(i -> i).toArray();

      @Override
      public int getNumberOfNodes() {
        return parentIndices.length;
      }

      @Override
      public int getParentId(int nodeIndex) {
        return parentIndices[nodeIndex];
      }

      @Override
      public int[] getChildrenIds(int nodeIndex) {
        return IntStream.range(1, parentIndices.length)
            .filter(i -> parentIndices[i] == nodeIndex)
            .sorted()
            .toArray();
      }
    };
  }

  TreeStructureBuilder() {
    tmpParentIndices = new ArrayList<>();
    tmpParentIndices.add(TreeStructure.NO_PARENT_ID);
  }

  @Override
  public Builder addNode(int parentIndex) {
    Preconditions.checkArgument(parentIndex >= 0);
    Preconditions.checkArgument(parentIndex < tmpParentIndices.size());
    tmpParentIndices.add(parentIndex);
    return this;
  }
}
