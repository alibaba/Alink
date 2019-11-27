package com.alibaba.alink.operator.common.statistics.basicstatistic;

import com.alibaba.alink.common.linalg.DenseMatrix;
import java.io.Serializable;

/**
 * Summarizer is the base class to calculate summary and store intermediate results, and Summary is the result of Summarizer.
 *
 * <p>Summarizer Inheritance relationship as follow:
 *         BaseSummarizer
 *            /       \
 *           /         \
 * TableSummarizer   BaseVectorSummarizer
 *                     /            \
 *                    /              \
 *     SparseVectorSummarizer    DenseVectorSummarizer
 *
 * <p>TableSummarizer is for table data, BaseVectorSummarizer is for vector data.
 *  SparseVectorSummarizer is for sparse vector, DenseVectorSummarizer is for dense vector.
 *
 *  <p>Summary Inheritance relationship as follow:
 *           BaseSummary
 *           /       \
 *         /         \
 *  TableSummary     BaseVectorSummary
 *                     /            \
 *                    /              \
 *     SparseVectorSummary    DenseVectorSummary
 *
 * <p>You can get statistics value from summary.
 *
 * <p>example:
 *      Row data =  Row.of("a", 1L, 1, 2.0, true)
 *      TableSummarizer summarizer = new TableSummarizer(selectedColNames, numberIdxs, bCov);
 *      summarizer.visit(data);
 *      TableSummary summary = summarizer.toSummary()
 *      double mean = summary.mean("col")
 */
public abstract class BaseSummarizer implements Serializable {

    /**
     * sum of outerProduct of each row.
     */
    DenseMatrix outerProduct;

    /**
     * count.
     */
    protected long count;

    /**
     * if calculateOuterProduct is true, outerProduct will be calculate. default not calculate.
     * If will be used to calculate correlation and covariance.
     */
    boolean calculateOuterProduct = false;

    /**
     * pearson correlation. https://en.wikipedia.org/wiki/Pearson_correlation_coefficient.
     * @return correlation as CorrelationResult.
     */
    public abstract CorrelationResult correlation();

    /**
     * covariance: https://en.wikipedia.org/wiki/Covariance.
     * @return covariance as matrix.
     */
    public abstract DenseMatrix covariance();


    /**
     * get outer product result as matrix.
     * @return outerProduct
     */
    public DenseMatrix getOuterProduct() {
        return outerProduct;
    }

}
