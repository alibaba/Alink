package com.alibaba.alink.operator.common.evaluation;

import java.io.Serializable;

/**
 * Base metrics for classification and regression evaluation.
 * <p>
 * All the evaluation metrics are calculated locally and merged through reduce function. Finally, they are saved as
 * params into BaseMetrics.
 */
public interface BaseMetricsSummary<T extends BaseMetrics<T>, M extends BaseMetricsSummary<T, M>>
    extends Cloneable, Serializable {

    /**
     * After merging all the BaseMetrics, we get the total BaseMetrics. Calculate the indexes and save them into
     * params and create the BaseMetrics.
     */
    T toMetrics();

    /**
     * Merge another metrics into itself.
     *
     * @param metrics Another metrics to merge.
     * @return itself.
     */
    M merge(M metrics);
}
