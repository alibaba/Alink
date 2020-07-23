package com.alibaba.alink.operator.common.evaluation;

import org.apache.flink.ml.api.misc.param.ParamInfo;

public enum TuningBinaryClassMetric {
    AUC(BinaryClassMetrics.AUC),
    KS(BinaryClassMetrics.KS),
    PRECISION(BinaryClassMetrics.PRECISION),
    RECALL(BinaryClassMetrics.RECALL),
    F1(BinaryClassMetrics.F1),
    LOG_LOSS(BinaryClassMetrics.LOG_LOSS),
    ACCURACY(BinaryClassMetrics.ACCURACY),
    MACRO_ACCURACY(BinaryClassMetrics.MACRO_ACCURACY),
    MICRO_ACCURACY(BinaryClassMetrics.MICRO_ACCURACY),
    WEIGHTED_ACCURACY(BinaryClassMetrics.WEIGHTED_ACCURACY),
    KAPPA(BinaryClassMetrics.KAPPA),
    MACRO_KAPPA(BinaryClassMetrics.MACRO_KAPPA),
    MICRO_KAPPA(BinaryClassMetrics.MICRO_KAPPA),
    WEIGHTED_KAPPA(BinaryClassMetrics.WEIGHTED_KAPPA),
    MACRO_PRECISION(BinaryClassMetrics.MACRO_PRECISION),
    MICRO_PRECISION(BinaryClassMetrics.MICRO_PRECISION),
    WEIGHTED_PRECISION(BinaryClassMetrics.WEIGHTED_PRECISION),
    MACRO_RECALL(BinaryClassMetrics.MACRO_RECALL),
    MICRO_RECALL(BinaryClassMetrics.MICRO_RECALL),
    WEIGHTED_RECALL(BinaryClassMetrics.WEIGHTED_RECALL),
    MACRO_F1(BinaryClassMetrics.MACRO_F1),
    MICRO_F1(BinaryClassMetrics.MICRO_F1),
    WEIGHTED_F1(BinaryClassMetrics.WEIGHTED_F1),
    MACRO_SENSITIVITY(BinaryClassMetrics.MACRO_SENSITIVITY),
    MICRO_SENSITIVITY(BinaryClassMetrics.MICRO_SENSITIVITY),
    WEIGHTED_SENSITIVITY(BinaryClassMetrics.WEIGHTED_SENSITIVITY),
    MACRO_SPECIFICITY(BinaryClassMetrics.MACRO_SPECIFICITY),
    MICRO_SPECIFICITY(BinaryClassMetrics.MICRO_SPECIFICITY),
    WEIGHTED_SPECIFICITY(BinaryClassMetrics.WEIGHTED_SPECIFICITY);

    private ParamInfo<Double> metricKey;

    TuningBinaryClassMetric(ParamInfo<Double> metricKey) {
        this.metricKey = metricKey;
    }

    public ParamInfo<Double> getMetricKey() {
        return metricKey;
    }
}
