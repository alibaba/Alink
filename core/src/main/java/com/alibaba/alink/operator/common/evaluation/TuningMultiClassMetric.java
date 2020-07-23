package com.alibaba.alink.operator.common.evaluation;

import org.apache.flink.ml.api.misc.param.ParamInfo;

public enum TuningMultiClassMetric {
    LOG_LOSS(MultiClassMetrics.LOG_LOSS),
    ACCURACY(MultiClassMetrics.ACCURACY),
    MACRO_ACCURACY(MultiClassMetrics.MACRO_ACCURACY),
    MICRO_ACCURACY(MultiClassMetrics.MICRO_ACCURACY),
    WEIGHTED_ACCURACY(MultiClassMetrics.WEIGHTED_ACCURACY),
    KAPPA(MultiClassMetrics.KAPPA),
    MACRO_KAPPA(MultiClassMetrics.MACRO_KAPPA),
    MICRO_KAPPA(MultiClassMetrics.MICRO_KAPPA),
    WEIGHTED_KAPPA(MultiClassMetrics.WEIGHTED_KAPPA),
    MACRO_PRECISION(MultiClassMetrics.MACRO_PRECISION),
    MICRO_PRECISION(MultiClassMetrics.MICRO_PRECISION),
    WEIGHTED_PRECISION(MultiClassMetrics.WEIGHTED_PRECISION),
    MACRO_RECALL(MultiClassMetrics.MACRO_RECALL),
    MICRO_RECALL(MultiClassMetrics.MICRO_RECALL),
    WEIGHTED_RECALL(MultiClassMetrics.WEIGHTED_RECALL),
    MACRO_F1(MultiClassMetrics.MACRO_F1),
    MICRO_F1(MultiClassMetrics.MICRO_F1),
    WEIGHTED_F1(MultiClassMetrics.WEIGHTED_F1),
    MACRO_SENSITIVITY(MultiClassMetrics.MACRO_SENSITIVITY),
    MICRO_SENSITIVITY(MultiClassMetrics.MICRO_SENSITIVITY),
    WEIGHTED_SENSITIVITY(MultiClassMetrics.WEIGHTED_SENSITIVITY),
    MACRO_SPECIFICITY(MultiClassMetrics.MACRO_SPECIFICITY),
    MICRO_SPECIFICITY(MultiClassMetrics.MICRO_SPECIFICITY),
    WEIGHTED_SPECIFICITY(MultiClassMetrics.WEIGHTED_SPECIFICITY);

    private ParamInfo<Double> metricKey;

    TuningMultiClassMetric(ParamInfo<Double> metricKey) {
        this.metricKey = metricKey;
    }

    public ParamInfo<Double> getMetricKey() {
        return metricKey;
    }
}
