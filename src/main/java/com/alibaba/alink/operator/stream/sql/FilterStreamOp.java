package com.alibaba.alink.operator.stream.sql;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.sql.FilterParams;
import org.apache.flink.ml.api.misc.param.Params;

/**
 * Filter records in the stream operator.
 */
public final class FilterStreamOp extends BaseSqlApiStreamOp<FilterStreamOp>
    implements FilterParams<FilterStreamOp> {

    public FilterStreamOp() {
        this(new Params());
    }

    public FilterStreamOp(String clause) {
        this(new Params().set(CLAUSE, clause));
    }

    public FilterStreamOp(Params params) {
        super(params);
    }

    @Override
    public FilterStreamOp linkFrom(StreamOperator<?>... inputs) {
        this.setOutputTable(inputs[0].filter(getClause()).getOutputTable());
        return this;
    }
}
