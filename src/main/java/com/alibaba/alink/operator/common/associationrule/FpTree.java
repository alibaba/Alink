package com.alibaba.alink.operator.common.associationrule;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.io.Serializable;

/**
 * FpTree defines the methods of a local FP Growth algorithm.
 * <p>
 * <p>Usage:
 * <p>
 * <pre>
 * FpTree fpTree = new FpTreeImpl();
 * fpTree.createTree();
 * for(int[] transaction : transactions) {
 *     fpTree.addTransaction(transaction);
 * }
 * fpTree.initialize();
 * fpTree.extractAll(...);
 * fpTree.destroyTree();
 * </pre>
 */
public interface FpTree extends Serializable {

    /**
     * Create the tree. Transactions can be added after the tree is created.
     */
    void createTree();

    /**
     * Destroy the tree. Free resources owned by the tree.
     */
    void destroyTree();

    /**
     * Add one transaction to the fp tree. The items in the transaction
     * should order descending by their supports.
     */
    void addTransaction(int[] transaction);

    /**
     * Initialize after all transactions have been added.
     */
    void initialize();

    /**
     * Print fp tree profile.
     */
    void printProfile();

    /**
     * Extract all patterns that end with each of those in "suffices".
     *
     * @param suffices         It specifies the suffices of patterns this method is going to explore.
     * @param minSupport       The minimum number of support.
     * @param maxPatternLength Max length of a pattern.
     * @param collector        The collector of patterns explored. A pattern is represented as an items array and its
     *                         support.
     */
    void extractAll(int[] suffices, int minSupport, int maxPatternLength, Collector<Tuple2<int[], Integer>> collector);

}
