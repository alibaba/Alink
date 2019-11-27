package com.alibaba.alink.operator.batch.sql;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.sql.FilterParams;

/**
 * Filter records in the batch operator.
 */
public final class FilterBatchOp extends BaseSqlApiBatchOp<FilterBatchOp>
    implements FilterParams<FilterBatchOp> {

    public FilterBatchOp() {
        this(new Params());
    }

    public FilterBatchOp(String clause) {
        this(new Params().set(CLAUSE, clause));
    }

    public FilterBatchOp(Params params) {
        super(params);
    }

    @Override
    public FilterBatchOp linkFrom(BatchOperator<?>... inputs) {
        this.setOutputTable(inputs[0].filter(getClause()).getOutputTable());
        return this;
    }
}
