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

import java.util.Objects;
import java.util.function.IntFunction;

public final class Tree<V> {

  private final TreeStructure treeStructure;

  private final Object[] data;

  public Tree(TreeStructure treeStructure) {
    this.treeStructure = Objects.requireNonNull(treeStructure);
    this.data = new Object[treeStructure.getNumberOfNodes()];
  }

  public Tree(TreeStructure treeStructure, IntFunction<V> initializer) {
    this(treeStructure);
    for (int i = 0; i < treeStructure.getNumberOfNodes(); ++i) {
      this.data[i] = initializer.apply(i);
    }
  }

  public V set(int nodeIndex, V value) {
    @SuppressWarnings("unchecked")
    V oldValue = (V) data[nodeIndex];
    data[nodeIndex] = value;
    return oldValue;
  }

  public V get(int nodeIndex) {
    @SuppressWarnings("unchecked")
    V ret = (V) data[nodeIndex];
    return ret;
  }

  public TreeStructure getTreeStructure() {
    return treeStructure;
  }
}
