package com.alibaba.alink.operator.batch.sql;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;

/**
 * Union with another <code>BatchOperator</code>. The duplicated records are kept.
 */
public final class UnionAllBatchOp extends BatchOperator<UnionAllBatchOp> {

    public UnionAllBatchOp() {
        this(new Params());
    }

    public UnionAllBatchOp(Params params) {
        super(params);
    }

    @Override
    public UnionAllBatchOp linkFrom(BatchOperator<?>... inputs) {
        checkMinOpSize(1, inputs);

        this.setOutputTable(inputs[0].getOutputTable());
        for (int i = 1; i < inputs.length; i++) {
            this.setOutputTable(this.getOutputTable().unionAll(inputs[i].getOutputTable()));
        }

        return this;
    }
}
