package com.alibaba.alink.operator.common.associationrule;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

/**
 * This is an efficient implementation of local FpGrowth algorithm.
 * Reference: Christian Borgelt, An Implementation of the FP-growth Algorithm.
 */
public class FpTreeImpl implements FpTree {
    private static final Logger LOG = LoggerFactory.getLogger(FpTreeImpl.class);

    /**
     * The tree node. Notice that no reference to children are kept.
     */
    private static class Node implements Serializable {
        int itemId;
        int support;
        Node parent;
        Node successor;
        Node auxPtr;

        public Node(int itemId, int support, Node parent) {
            this.itemId = itemId;
            this.support = support;
            this.parent = parent;
            this.successor = null;
            this.auxPtr = null;
        }
    }

    /**
     * Summary of an item in the Fp-tree.
     */
    private static class Summary implements Serializable {
        /**
         * Number of nodes in the tree.
         */
        int count;

        /**
         * The head of the linked list of all nodes of an item.
         */
        Node head;

        public Summary(Node head) {
            this.head = head;
        }

        public void countAll() {
            Node p = head;
            count = 0;
            while (p != null) {
                count += p.support;
                p = p.successor;
            }
        }

        @Override
        public String toString() {
            StringBuilder sbd = new StringBuilder();
            Node p = head;
            while (p != null) {
                sbd.append("->").append(String.format("(%d,%d,%d)",
                    p.itemId, p.support, p.parent == null ? -1 : p.parent.itemId));
                p = p.successor;
            }
            return sbd.toString();
        }
    }

    private Map<Integer, Summary> summaries; // item -> summary of the item

    // transient data for building trees.
    private Map<Integer, Node> roots; // item -> root node of the item
    private Map<Integer, List<Node>> itemNodes; // item -> list of nodes of the item

    public FpTreeImpl() {
    }

    private FpTreeImpl(Map<Integer, Summary> summaries) {
        this.summaries = summaries;
        this.summaries.forEach((itemId, summary) -> {
            summary.countAll();
        });
    }

    @Override
    public void createTree() {
        this.summaries = new HashMap<>();
        this.roots = new HashMap<>();
        this.itemNodes = new HashMap<>();
    }

    @Override
    public void destroyTree() {
        if (summaries != null) {
            this.summaries.clear();
        }
        if (roots != null) {
            this.roots.clear();
        }
        if (itemNodes != null) {
            this.itemNodes.clear();
        }
    }

    @Override
    public void addTransaction(int[] transaction) {
        if (transaction.length == 0) {
            return;
        }
        int firstItem = transaction[0];
        Node curr;
        if (roots.containsKey(firstItem)) {
            curr = roots.get(firstItem);
            curr.support += 1;
        } else {
            curr = new Node(firstItem, 1, null);
            List<Node> list = new ArrayList<>();
            list.add(curr);
            itemNodes.merge(firstItem, list, (old, delta) -> {
                old.addAll(delta);
                return old;
            });
            roots.put(firstItem, curr);
        }

        for (int i = 1; i < transaction.length; i++) {
            int item = transaction[i];
            Node p = curr.auxPtr; // use auxPtr as head of siblings
            while (p != null && p.itemId != item) {
                p = p.successor;
            }
            if (p != null) { // found
                p.support += 1;
                curr = p;
            } else { // not found
                Node newNode = new Node(item, 1, curr);
                newNode.successor = curr.auxPtr;
                curr.auxPtr = newNode;
                curr = newNode;
                List<Node> list = new ArrayList<>();
                list.add(newNode);
                itemNodes.merge(item, list, (old, delta) -> {
                    old.addAll(delta);
                    return old;
                });
            }
        }
    }

    @Override
    public void initialize() {
        this.itemNodes.forEach((item, nodesList) -> {
            int n = nodesList.size();
            for (int i = 0; i < n; i++) {
                Node curr = nodesList.get(i);
                curr.auxPtr = null;
                curr.successor = (i + 1) >= n ? null : nodesList.get(i + 1);
            }
            this.summaries.put(item, new Summary(nodesList.get(0)));
        });

        // clear data buffer
        this.roots.clear();
        this.itemNodes.clear();

        this.summaries.forEach((item, summary) -> summary.countAll());
    }

    /**
     * Project the tree on the given item.
     */
    private FpTreeImpl project(int itemId, int minSupportCnt) {
        if (!this.summaries.containsKey(itemId)) {
            throw new RuntimeException("not contain item " + itemId);
        }
        Summary summary = this.summaries.get(itemId);
        Map<Integer, Summary> projectedSummaries = new HashMap<>();

        Node p = summary.head;
        while (p != null) {
            // trace upward
            Node f = p.parent;
            while (f != null) {
                if (f.auxPtr == null) {
                    f.auxPtr = new Node(f.itemId, p.support, null);
                    if (projectedSummaries.containsKey(f.auxPtr.itemId)) {
                        Summary summary0 = projectedSummaries.get(f.auxPtr.itemId);
                        f.auxPtr.successor = summary0.head;
                        summary0.head = f.auxPtr;
                    } else {
                        Summary summary0 = new Summary(f.auxPtr);
                        projectedSummaries.put(f.auxPtr.itemId, summary0);
                    }
                } else { // aux ptr already created by another branch
                    f.auxPtr.support += p.support;
                }
                f = f.parent;
            }
            p = p.successor;
        }

        p = summary.head;
        while (p != null) {
            // trace upward again to set parent ptr and clear auxPtr
            Node f = p.parent;
            while (f != null) {
                if (f.parent != null) {
                    f.auxPtr.parent = f.parent.auxPtr;
                }
                f = f.parent;
            }
            p = p.successor;
        }

        // prune
        Set<Integer> toPrune = new HashSet<>();
        projectedSummaries.forEach((item, s) -> {
            s.countAll();
            if (s.count < minSupportCnt) {
                toPrune.add(item);
            }
        });
        toPrune.forEach(projectedSummaries::remove);

        p = summary.head;
        while (p != null) {
            Node f = p.parent;
            if (f != null) {
                Node leaf = f.auxPtr;
                while (leaf != null && toPrune.contains(leaf.itemId)) {
                    leaf = leaf.parent;
                }
                while (leaf != null) {
                    Node leafParent = leaf.parent;
                    while (leafParent != null && toPrune.contains(leafParent.itemId)) {
                        leafParent = leafParent.parent;
                    }
                    leaf.parent = leafParent;
                    leaf = leafParent;
                }
            }
            p = p.successor;
        }

        // clear auxPtr
        p = summary.head;
        while (p != null) {
            Node f = p.parent;
            while (f != null) {
                f.auxPtr = null;
                f = f.parent;
            }
            p = p.successor;
        }

        return new FpTreeImpl(projectedSummaries);
    }

    private void extractImpl(int minSupportCnt, int item, int maxLength, int[] suffix,
                             Collector<Tuple2<int[], Integer>> collector) {
        if (maxLength < 1) {
            return;
        }
        Summary summary = summaries.get(item);
        if (summary.count < minSupportCnt) {
            return;
        }
        int[] newSuffix = new int[suffix.length + 1];
        newSuffix[0] = item;
        System.arraycopy(suffix, 0, newSuffix, 1, suffix.length);
        collector.collect(Tuple2.of(newSuffix.clone(), summary.count));
        if (maxLength == 1) {
            return;
        }
        FpTreeImpl projectedTree = this.project(item, minSupportCnt);
        projectedTree.summaries.forEach((pItem, pSummary) -> {
            projectedTree.extractImpl(minSupportCnt, pItem, maxLength - 1, newSuffix, collector);
        });
    }

    @Override
    public void extractAll(int[] suffices, int minSupport, int maxPatternLength,
                           Collector<Tuple2<int[], Integer>> collector) {
        for (int item : suffices) {
            extractImpl(minSupport, item, maxPatternLength, new int[0], collector);
        }
    }

    /**
     * Print the tree for debugging purpose.
     */
    public void print() {
        summaries.forEach((item, summary) -> {
            System.out.println(String.format("Summary(item=%d,count=%d):%s", item, summary.count, summary.toString()));
        });
    }

    /**
     * Print the tree profile for debugging purpose.
     */
    @Override
    public void printProfile() {
        // tuple:
        // 1) num distinct items in the tree,
        // 2) sum of support of each items,
        // 3) num tree nodes in the tree
        Tuple3<Integer, Integer, Integer> counts = Tuple3.of(0, 0, 0);
        summaries.forEach((item, summary) -> {
            counts.f0 += 1;
            counts.f1 += summary.count;
            Node p = summary.head;
            while (p != null) {
                counts.f2 += 1;
                p = p.successor;
            }
        });
        System.out.println("fptree_profile: " + counts);
        LOG.info("fptree_profile {}", counts);
    }
}
