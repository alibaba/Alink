package com.alibaba.alink.operator.batch.sql;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;

/**
 * Minus another <code>BatchOperator</code>.
 */
public final class MinusBatchOp extends BatchOperator<MinusBatchOp> {

    public MinusBatchOp() {
        this(new Params());
    }

    public MinusBatchOp(Params params) {
        super(params);
    }

    @Override
    public MinusBatchOp linkFrom(BatchOperator<?>... inputs) {
        checkOpSize(2, inputs);
        this.setOutputTable(inputs[0].getOutputTable().minus(inputs[1].getOutputTable()));
        return this;
    }
}
