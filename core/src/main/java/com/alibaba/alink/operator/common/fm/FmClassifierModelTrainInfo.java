package com.alibaba.alink.operator.common.fm;

import java.util.List;

import org.apache.flink.types.Row;

/**
 * Fm classifier model train info.
 */
public final class FmClassifierModelTrainInfo extends FmRegressorModelTrainInfo {

    public FmClassifierModelTrainInfo(List<Row> rows) {
        super(rows);
    }

    @Override
    protected void setKeys() {
        keys = new String[] {" auc: ", " accuracy: "};
    }
}
