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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class TreeUtilTest {

  @Test
  public void testCreateBalancedBinaryTree() {
    TreeStructure treeStructure = TreeUtil.createBalancedBinaryTree(10);

    assertEquals(10, treeStructure.getNumberOfNodes());

    assertEquals(TreeStructure.NO_PARENT_ID, treeStructure.getParentId(0));
    assertEquals(TreeStructure.ROOT_ID, treeStructure.getParentId(1));
    assertEquals(TreeStructure.ROOT_ID, treeStructure.getParentId(2));
    assertEquals(1, treeStructure.getParentId(3));
    assertEquals(1, treeStructure.getParentId(4));
    assertEquals(2, treeStructure.getParentId(5));
    assertEquals(2, treeStructure.getParentId(6));
    assertEquals(3, treeStructure.getParentId(7));
    assertEquals(3, treeStructure.getParentId(8));
    assertEquals(4, treeStructure.getParentId(9));

    assertArrayEquals(new int[] {1, 2}, treeStructure.getChildrenIds(0));
    assertArrayEquals(new int[] {3, 4}, treeStructure.getChildrenIds(1));
    assertArrayEquals(new int[] {5, 6}, treeStructure.getChildrenIds(2));
    assertArrayEquals(new int[] {7, 8}, treeStructure.getChildrenIds(3));
    assertArrayEquals(new int[] {9}, treeStructure.getChildrenIds(4));
    assertArrayEquals(new int[] {}, treeStructure.getChildrenIds(5));
    assertArrayEquals(new int[] {}, treeStructure.getChildrenIds(6));
    assertArrayEquals(new int[] {}, treeStructure.getChildrenIds(7));
    assertArrayEquals(new int[] {}, treeStructure.getChildrenIds(8));
    assertArrayEquals(new int[] {}, treeStructure.getChildrenIds(9));
  }

  @Test
  public void testCreateChain() {
    TreeStructure treeStructure = TreeUtil.createChain(10);

    assertEquals(10, treeStructure.getNumberOfNodes());

    assertEquals(TreeStructure.NO_PARENT_ID, treeStructure.getParentId(0));
    assertEquals(0, treeStructure.getParentId(1));
    assertEquals(1, treeStructure.getParentId(2));
    assertEquals(2, treeStructure.getParentId(3));
    assertEquals(3, treeStructure.getParentId(4));
    assertEquals(4, treeStructure.getParentId(5));
    assertEquals(5, treeStructure.getParentId(6));
    assertEquals(6, treeStructure.getParentId(7));
    assertEquals(7, treeStructure.getParentId(8));
    assertEquals(8, treeStructure.getParentId(9));

    assertArrayEquals(new int[] {1}, treeStructure.getChildrenIds(0));
    assertArrayEquals(new int[] {2}, treeStructure.getChildrenIds(1));
    assertArrayEquals(new int[] {3}, treeStructure.getChildrenIds(2));
    assertArrayEquals(new int[] {4}, treeStructure.getChildrenIds(3));
    assertArrayEquals(new int[] {5}, treeStructure.getChildrenIds(4));
    assertArrayEquals(new int[] {6}, treeStructure.getChildrenIds(5));
    assertArrayEquals(new int[] {7}, treeStructure.getChildrenIds(6));
    assertArrayEquals(new int[] {8}, treeStructure.getChildrenIds(7));
    assertArrayEquals(new int[] {9}, treeStructure.getChildrenIds(8));
    assertArrayEquals(new int[] {}, treeStructure.getChildrenIds(9));
  }

  @Test
  public void testGetLevel() {
    TreeStructure treeStructure = TreeUtil.createBalancedBinaryTree(17);

    assertEquals(0, TreeUtil.getLevel(treeStructure, 0));
    assertEquals(1, TreeUtil.getLevel(treeStructure, 1));
    assertEquals(1, TreeUtil.getLevel(treeStructure, 2));
    assertEquals(2, TreeUtil.getLevel(treeStructure, 3));
    assertEquals(2, TreeUtil.getLevel(treeStructure, 4));
    assertEquals(2, TreeUtil.getLevel(treeStructure, 5));
    assertEquals(2, TreeUtil.getLevel(treeStructure, 6));
    assertEquals(3, TreeUtil.getLevel(treeStructure, 7));
    assertEquals(3, TreeUtil.getLevel(treeStructure, 8));
    assertEquals(3, TreeUtil.getLevel(treeStructure, 9));
    assertEquals(3, TreeUtil.getLevel(treeStructure, 10));
    assertEquals(3, TreeUtil.getLevel(treeStructure, 11));
    assertEquals(3, TreeUtil.getLevel(treeStructure, 12));
    assertEquals(3, TreeUtil.getLevel(treeStructure, 13));
    assertEquals(3, TreeUtil.getLevel(treeStructure, 14));
    assertEquals(4, TreeUtil.getLevel(treeStructure, 15));
    assertEquals(4, TreeUtil.getLevel(treeStructure, 16));
  }

  @Test
  public void testPrintBinaryTree() {
    TreeStructure treeStructure = TreeUtil.createBalancedBinaryTree(50);

    String expected =
        "\n"
            + "0\n"
            + "|---1\n"
            + "|   |---3\n"
            + "|   |   |---7\n"
            + "|   |   |   |---15\n"
            + "|   |   |   |   |---31\n"
            + "|   |   |   |   '---32\n"
            + "|   |   |   '---16\n"
            + "|   |   |       |---33\n"
            + "|   |   |       '---34\n"
            + "|   |   '---8\n"
            + "|   |       |---17\n"
            + "|   |       |   |---35\n"
            + "|   |       |   '---36\n"
            + "|   |       '---18\n"
            + "|   |           |---37\n"
            + "|   |           '---38\n"
            + "|   '---4\n"
            + "|       |---9\n"
            + "|       |   |---19\n"
            + "|       |   |   |---39\n"
            + "|       |   |   '---40\n"
            + "|       |   '---20\n"
            + "|       |       |---41\n"
            + "|       |       '---42\n"
            + "|       '---10\n"
            + "|           |---21\n"
            + "|           |   |---43\n"
            + "|           |   '---44\n"
            + "|           '---22\n"
            + "|               |---45\n"
            + "|               '---46\n"
            + "'---2\n"
            + "    |---5\n"
            + "    |   |---11\n"
            + "    |   |   |---23\n"
            + "    |   |   |   |---47\n"
            + "    |   |   |   '---48\n"
            + "    |   |   '---24\n"
            + "    |   |       '---49\n"
            + "    |   '---12\n"
            + "    |       |---25\n"
            + "    |       '---26\n"
            + "    '---6\n"
            + "        |---13\n"
            + "        |   |---27\n"
            + "        |   '---28\n"
            + "        '---14\n"
            + "            |---29\n"
            + "            '---30";
    assertEquals(expected, TreeUtil.printStructure(treeStructure, Integer::toString));
  }

  @Test
  public void testPrintChain() {
    TreeStructure treeStructure = TreeUtil.createChain(15);

    String expected =
        "\n"
            + "0\n"
            + "'---1\n"
            + "    '---2\n"
            + "        '---3\n"
            + "            '---4\n"
            + "                '---5\n"
            + "                    '---6\n"
            + "                        '---7\n"
            + "                            '---8\n"
            + "                                '---9\n"
            + "                                    '---10\n"
            + "                                        '---11\n"
            + "                                            '---12\n"
            + "                                                '---13\n"
            + "                                                    '---14";
    assertEquals(expected, TreeUtil.printStructure(treeStructure, Integer::toString));
  }

  @Test
  public void testPrintTernaryTree() {
    TreeStructure treeStructure = TreeUtil.createBalancedTree(21, 3);

    String expected =
        "\n"
            + "0\n"
            + "|---1\n"
            + "|   |---4\n"
            + "|   |   |---13\n"
            + "|   |   |---14\n"
            + "|   |   '---15\n"
            + "|   |---5\n"
            + "|   |   |---16\n"
            + "|   |   |---17\n"
            + "|   |   '---18\n"
            + "|   '---6\n"
            + "|       |---19\n"
            + "|       '---20\n"
            + "|---2\n"
            + "|   |---7\n"
            + "|   |---8\n"
            + "|   '---9\n"
            + "'---3\n"
            + "    |---10\n"
            + "    |---11\n"
            + "    '---12";
    assertEquals(expected, TreeUtil.printStructure(treeStructure, Integer::toString));
  }

  @Test
  public void testPrintRandomTree() {
    TreeStructure treeStructure = TreeUtil.generateRandomTree(0L, 47);

    String expected =
        "\n"
            + "0\n"
            + "|---1\n"
            + "|   |---2\n"
            + "|   |   |---4\n"
            + "|   |   |   |---5\n"
            + "|   |   |   |   |---11\n"
            + "|   |   |   |   |   |---12\n"
            + "|   |   |   |   |   |---26\n"
            + "|   |   |   |   |   '---40\n"
            + "|   |   |   |   |       '---46\n"
            + "|   |   |   |   |---13\n"
            + "|   |   |   |   |   |---29\n"
            + "|   |   |   |   |   '---45\n"
            + "|   |   |   |   '---19\n"
            + "|   |   |   |       '---37\n"
            + "|   |   |   '---6\n"
            + "|   |   '---25\n"
            + "|   |       '---34\n"
            + "|   |---8\n"
            + "|   |   |---14\n"
            + "|   |   |   |---18\n"
            + "|   |   |   |   '---27\n"
            + "|   |   |   |       '---28\n"
            + "|   |   |   |---21\n"
            + "|   |   |   |   '---31\n"
            + "|   |   |   |       '---41\n"
            + "|   |   |   |---35\n"
            + "|   |   |   '---44\n"
            + "|   |   |---36\n"
            + "|   |   '---43\n"
            + "|   |---9\n"
            + "|   |   |---10\n"
            + "|   |   |   |---15\n"
            + "|   |   |   |   |---23\n"
            + "|   |   |   |   '---33\n"
            + "|   |   |   |---16\n"
            + "|   |   |   |---20\n"
            + "|   |   |   '---24\n"
            + "|   |   |---17\n"
            + "|   |   |---22\n"
            + "|   |   '---39\n"
            + "|   |---30\n"
            + "|   '---38\n"
            + "|       '---42\n"
            + "|---3\n"
            + "|   '---7\n"
            + "'---32";
    assertEquals(expected, TreeUtil.printStructure(treeStructure, Integer::toString));
  }
}
