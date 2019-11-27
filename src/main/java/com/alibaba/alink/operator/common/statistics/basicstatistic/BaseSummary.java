package com.alibaba.alink.operator.common.statistics.basicstatistic;

/**
 * Summarizer is the base class to calculate summary and store intermediate results, and Summary is the result of Summarizer.
 *
 * <p>Summarizer Inheritance relationship as follow:
 *         BaseSummarizer
 *            /       \
 *          /         \
 * TableSummarizer   BaseVectorSummarizer
 *                     /            \
 *                    /              \
 *      SparseVectorSummarizer    DenseVectorSummarizer
 *
 * <p> TableSummarizer is for table data, BaseVectorSummarizer is for vector data.
 *  SparseVectorSummarizer is for sparse vector, DenseVectorSummarizer is for dense vector.
 *
 * <p> Summary Inheritance relationship as follow:
 *            BaseSummary
 *            /       \
 *           /         \
 *  TableSummary     BaseVectorSummary
 *                     /            \
 *                    /              \
 *      SparseVectorSummary    DenseVectorSummary
 *
 * <p> You can get statistics value from summary.
 *
 * <p> example:
 *      Row data =  Row.of("a", 1L, 1, 2.0, true)
 *      TableSummarizer summarizer = new TableSummarizer(selectedColNames, numberIdxs, bCov);
 *      summarizer = summarizer.visit(data);
 *      TableSummary summary = summarizer.toSummary()
 *      double mean = summary.mean("col")
 */
public abstract class BaseSummary {

    /**
     * count.
     */
    protected long count;

    /**
     * count.
     */
    public long count() {
        return count;
    }

}
