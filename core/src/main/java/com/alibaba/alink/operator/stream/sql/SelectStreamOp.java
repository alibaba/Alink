package com.alibaba.alink.operator.stream.sql;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.sql.SelectParams;

/**
 * Select the fields of a stream operator.
 */
public final class SelectStreamOp extends BaseSqlApiStreamOp<SelectStreamOp>
    implements SelectParams<SelectStreamOp> {

    public SelectStreamOp() {
        this(new Params());
    }

    public SelectStreamOp(String clause) {
        this(new Params().set(CLAUSE, clause));
    }

    public SelectStreamOp(Params params) {
        super(params);
    }

    @Override
    public SelectStreamOp linkFrom(StreamOperator<?>... inputs) {
        this.setOutputTable(inputs[0].select(getClause()).getOutputTable());
        return this;
    }
}

