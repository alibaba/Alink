package com.alibaba.alink.operator.stream.sql;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.sql.AsParams;

/**
 * Rename the fields of a stream operator.
 */
public final class AsStreamOp extends BaseSqlApiStreamOp<AsStreamOp>
    implements AsParams<AsStreamOp> {

    public AsStreamOp() {
        this(new Params());
    }

    public AsStreamOp(String clause) {
        this(new Params().set(CLAUSE, clause));
    }

    public AsStreamOp(Params params) {
        super(params);
    }

    @Override
    public AsStreamOp linkFrom(StreamOperator<?>... inputs) {
        this.setOutputTable(inputs[0].as(getClause()).getOutputTable());
        return this;
    }
}