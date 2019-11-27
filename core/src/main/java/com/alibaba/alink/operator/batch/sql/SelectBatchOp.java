package com.alibaba.alink.operator.batch.sql;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.sql.SelectParams;

/**
 * Select the fields of a batch operator.
 */
public final class SelectBatchOp extends BaseSqlApiBatchOp<SelectBatchOp>
    implements SelectParams<SelectBatchOp> {

    public SelectBatchOp() {
        this(new Params());
    }

    public SelectBatchOp(String clause) {
        this(new Params().set(CLAUSE, clause));
    }

    public SelectBatchOp(Params params) {
        super(params);
    }

    @Override
    public SelectBatchOp linkFrom(BatchOperator<?>... inputs) {
        this.setOutputTable(inputs[0].select(getClause()).getOutputTable());
        return this;
    }
}
