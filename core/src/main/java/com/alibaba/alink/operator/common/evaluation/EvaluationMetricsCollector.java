package com.alibaba.alink.operator.common.evaluation;

/**
 * Collector the evaluation metrics to local.
 */
public interface EvaluationMetricsCollector<T extends BaseMetrics> {
    public T collectMetrics();
}
