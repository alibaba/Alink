package com.alibaba.alink.operator.common.linear;

import java.util.List;

import org.apache.flink.types.Row;

/**
 * Linear classifier (lr, svm) model info.
 */
public class LinearClassifierModelInfo extends LinearRegressorModelInfo {

    public Object[] getLabelValues() {
        return this.labelValues;
    }

    public LinearClassifierModelInfo(List<Row> rows) {
        super(rows);
    }

    @Override
    protected void processLabelValues(LinearModelData modelData) {
        this.labelValues = modelData.labelValues;
    }
}
