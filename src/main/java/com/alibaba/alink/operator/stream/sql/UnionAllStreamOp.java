package com.alibaba.alink.operator.stream.sql;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.stream.StreamOperator;

/**
 * Union two stream operators. The duplicated records are kept.
 */
public final class UnionAllStreamOp extends StreamOperator<UnionAllStreamOp> {

    public UnionAllStreamOp() {
        this(new Params());
    }

    public UnionAllStreamOp(Params params) {
        super(params);
    }

    @Override
    public UnionAllStreamOp linkFrom(StreamOperator<?>... inputs) {
        checkMinOpSize(1, inputs);

        this.setOutputTable(inputs[0].getOutputTable());
        for (int i = 1; i < inputs.length; i++) {
            this.setOutputTable(this.getOutputTable().unionAll(inputs[i].getOutputTable()));
        }

        return this;
    }
}
