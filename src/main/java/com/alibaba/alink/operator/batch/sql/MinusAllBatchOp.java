package com.alibaba.alink.operator.batch.sql;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;

/**
 * Minus another <code>BatchOperator</code>. The duplicated records are kept.
 */
public final class MinusAllBatchOp extends BatchOperator<MinusAllBatchOp> {

    public MinusAllBatchOp() {
        this(new Params());
    }

    public MinusAllBatchOp(Params params) {
        super(params);
    }

    @Override
    public MinusAllBatchOp linkFrom(BatchOperator<?>... inputs) {
        checkMinOpSize(2, inputs);
        this.setOutputTable(inputs[0].getOutputTable().minusAll(inputs[1].getOutputTable()));
        return this;
    }
}
