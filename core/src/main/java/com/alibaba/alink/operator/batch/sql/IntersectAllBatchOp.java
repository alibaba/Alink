package com.alibaba.alink.operator.batch.sql;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;

/**
 * Intersect two batch  operators. It returns records that exist in both batch
 * operators. Duplicated records are kept.
 */
public final class IntersectAllBatchOp extends BatchOperator<IntersectAllBatchOp> {
    public IntersectAllBatchOp() {
        this(new Params());
    }

    public IntersectAllBatchOp(Params params) {
        super(params);
    }

    @Override
    public IntersectAllBatchOp linkFrom(BatchOperator<?>... inputs) {
        checkOpSize(2, inputs);

        this.setOutputTable(inputs[0].getOutputTable().intersectAll(inputs[1].getOutputTable()));
        return this;
    }
}
