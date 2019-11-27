package com.alibaba.alink.operator.batch.sql;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.sql.BatchSqlOperators;
import com.alibaba.alink.params.sql.JoinParams;
import org.apache.flink.ml.api.misc.param.Params;

/**
 * Right outer join two batch operators.
 */
public final class RightOuterJoinBatchOp extends BaseSqlApiBatchOp<RightOuterJoinBatchOp>
    implements JoinParams<RightOuterJoinBatchOp> {

    public RightOuterJoinBatchOp() {
        this(new Params());
    }

    public RightOuterJoinBatchOp(String whereClause, String selectClause) {
        this(new Params()
            .set(JOIN_PREDICATE, whereClause)
            .set(SELECT_CLAUSE, selectClause));
    }

    public RightOuterJoinBatchOp(Params params) {
        super(params);
    }

    @Override
    public RightOuterJoinBatchOp linkFrom(BatchOperator<?>... inputs) {
        String joinPredicate = getJoinPredicate();
        String selectClause = getSelectClause();
        this.setOutputTable(BatchSqlOperators.rightOuterJoin(inputs[0], inputs[1], joinPredicate, selectClause)
            .getOutputTable());
        return this;
    }
}
