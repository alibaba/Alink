package com.alibaba.alink.operator.batch.sql;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;

/**
 * Intersect two batch  operators. It returns records that exist in both batch
 * operators. Duplicated records are removed.
 */
public final class IntersectBatchOp extends BatchOperator<IntersectBatchOp> {

    public IntersectBatchOp() {
        this(new Params());
    }

    public IntersectBatchOp(Params params) {
        super(params);
    }

    @Override
    public IntersectBatchOp linkFrom(BatchOperator<?>... inputs) {
        checkOpSize(2, inputs);
        this.setOutputTable(inputs[0].getOutputTable().intersect(inputs[1].getOutputTable()));
        return this;
    }
}
