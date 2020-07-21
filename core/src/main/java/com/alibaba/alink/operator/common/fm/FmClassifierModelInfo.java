package com.alibaba.alink.operator.common.fm;

import java.util.List;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;

/**
 * Model info of FmClassifier.
 */
public class FmClassifierModelInfo extends FmRegressorModelInfo {

    public Object[] getLabelValues() {
        return this.labelValues;
    }

    public FmClassifierModelInfo(List<Row> rows, TypeInformation labelType) {
        super(rows, labelType);
    }

    @Override
    protected void processLabelValues(FmModelData modelData) {
        this.labelValues = modelData.labelValues;
    }
}
