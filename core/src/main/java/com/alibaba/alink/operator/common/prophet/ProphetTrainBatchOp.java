package com.alibaba.alink.operator.common.prophet;

import com.alibaba.alink.common.lazy.WithModelInfoBatchOp;
import com.alibaba.alink.operator.batch.BatchOperator;

/**
 * the real batch train op
 */
public class ProphetTrainBatchOp extends BatchOperator<ProphetTrainBatchOp>
        implements ProphetTrainParams<ProphetTrainBatchOp>,
        WithModelInfoBatchOp<ProphetModelInfo, ProphetTrainBatchOp, ProphetModelInfoBatchOp> {

    @Override
    public ProphetModelInfoBatchOp getModelInfoBatchOp() {
        return new ProphetModelInfoBatchOp(this.getParams()).linkFrom(this);
    }

    @Override
    public ProphetTrainBatchOp linkFrom(BatchOperator<?>... inputs) {
        BatchOperator<?> in = checkAndGetFirst(inputs);
        String[] selectedColNames = getSelectedCols();

        ProphetModelDataConverter converter = new ProphetModelDataConverter();

        this.setOutput(in.getDataSet().mapPartition(new BuildProphetModel(selectedColNames))
                .setParallelism(1), converter.getModelSchema());

        return this;
    }
}
