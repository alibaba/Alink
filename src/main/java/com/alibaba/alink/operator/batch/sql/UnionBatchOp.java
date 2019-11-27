package com.alibaba.alink.operator.batch.sql;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;

/**
 * Union with other <code>BatchOperator</code>s.
 */
public final class UnionBatchOp extends BatchOperator<UnionBatchOp> {

    public UnionBatchOp() {
        this(new Params());
    }

    public UnionBatchOp(Params params) {
        super(params);
    }

    @Override
    public UnionBatchOp linkFrom(BatchOperator<?>... inputs) {
        checkMinOpSize(1, inputs);
        this.setOutputTable(inputs[0].getOutputTable());
        for (int i = 1; i < inputs.length; i++) {
            this.setOutputTable(this.getOutputTable().union(inputs[i].getOutputTable()));
        }
        return this;
    }
}
