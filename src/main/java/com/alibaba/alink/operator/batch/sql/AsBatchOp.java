package com.alibaba.alink.operator.batch.sql;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.sql.AsParams;

/**
 * Rename the fields of a batch operator.
 */
public final class AsBatchOp extends BaseSqlApiBatchOp<AsBatchOp>
    implements AsParams<AsBatchOp> {

    public AsBatchOp() {
        this(new Params());
    }

    public AsBatchOp(String clause) {
        this(new Params().set(CLAUSE, clause));
    }

    public AsBatchOp(Params params) {
        super(params);
    }

    @Override
    public AsBatchOp linkFrom(BatchOperator<?>... inputs) {
        this.setOutputTable(inputs[0].as(getClause()).getOutputTable());
        return this;
    }
}
