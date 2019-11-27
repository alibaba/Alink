package com.alibaba.alink.operator.batch.sql;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.sql.BatchSqlOperators;
import com.alibaba.alink.params.sql.JoinParams;
import org.apache.flink.ml.api.misc.param.Params;

/**
 * Full outer join two batch operators.
 */
public final class FullOuterJoinBatchOp extends BaseSqlApiBatchOp<FullOuterJoinBatchOp>
    implements JoinParams<FullOuterJoinBatchOp> {

    public FullOuterJoinBatchOp() {
        this(new Params());
    }

    public FullOuterJoinBatchOp(String whereClause, String selectClause) {
        this(new Params()
            .set(JoinParams.JOIN_PREDICATE, whereClause)
            .set(JoinParams.SELECT_CLAUSE, selectClause));
    }

    public FullOuterJoinBatchOp(Params params) {
        super(params);
    }

    @Override
    public FullOuterJoinBatchOp linkFrom(BatchOperator<?>... inputs) {
        String joinPredicate = getJoinPredicate();
        String selectClause = getSelectClause();
        this.setOutputTable(BatchSqlOperators.fullOuterJoin(inputs[0], inputs[1], joinPredicate, selectClause).getOutputTable());
        return this;
    }
}
