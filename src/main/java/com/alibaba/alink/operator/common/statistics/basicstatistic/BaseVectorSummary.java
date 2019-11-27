package com.alibaba.alink.operator.common.statistics.basicstatistic;

import com.alibaba.alink.common.linalg.Vector;

/**
 * It is the base class which is used to store vector summary result.
 * You can get vectorSize, sum, mean, variance, standardDeviation, and so on.
 *
 * <p>Summary Inheritance relationship as follow:
 *            BaseSummary
 *             /       \
 *            /         \
 *   TableSummary     BaseVectorSummary
 *                     /            \
 *                    /              \
 *      SparseVectorSummary    DenseVectorSummary
 *
 * <p>It can use toSummary() to get the result BaseVectorSummary.
 *
 * <p>example:
 *
 *   DenseVector data = new DenseVector(new double[]{1.0, -1.0, 3.0})
 *   DenseVectorSummarizer summarizer = new DenseVectorSummarizer(false);
 *   summarizer = summarizer.visit(data);
 *   BaseVectorSummary summary = summarizer.toSummary()
 *   double mean = summary.mean(0)
 *
 */
public abstract class BaseVectorSummary extends BaseSummary {

    /**
     * vector size.
     */
    public abstract int vectorSize();

    /**
     * sum of each dimension.
     */
    public abstract Vector sum();

    /**
     * mean of each feature.
     */
    public abstract Vector mean();

    /**
     * variance of each feature.
     */
    public abstract Vector variance();

    /**
     * standardDeviation of each feature.
     */
    public abstract Vector standardDeviation();

    /**
     * min of each feature.
     */
    public abstract Vector min();

    /**
     * max of each feature.
     */
    public abstract Vector max();

    /**
     * l1 norm of each feature.
     */
    public abstract Vector normL1();

    /**
     * Euclidean norm of each feature.
     */
    public abstract Vector normL2();

    /**
     * return sum of idx feature
     */
    public double sum(int idx) {
        return sum().get(idx);
    }

    /**
     * return mean of idx feature.
     */
    public double mean(int idx) {
        return mean().get(idx);
    }

    /**
     * return variance of idx feature.
     */
    public double variance(int idx) {
        return variance().get(idx);
    }

    /**
     * return standardDeviation of idx feature.
     */
    public double standardDeviation(int idx) {
        return standardDeviation().get(idx);
    }

    /**
     * return min of idx feature.
     */
    public double min(int idx) {
        return min().get(idx);
    }

    /**
     * return max of idx feature.
     */
    public double max(int idx) {
        return max().get(idx);
    }

    /**
     * return normL1 of idx feature.
     */
    public double normL1(int idx) {
        return normL1().get(idx);
    }

    /**
     * return normL2 of idx feature.
     */
    public double normL2(int idx) {
        return normL2().get(idx);
    }

}
