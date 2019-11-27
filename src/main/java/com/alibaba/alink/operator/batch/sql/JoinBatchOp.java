package com.alibaba.alink.operator.batch.sql;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.sql.BatchSqlOperators;
import com.alibaba.alink.params.sql.JoinParams;

import java.io.Serializable;

/**
 * Join two batch operators.
 */
public final class JoinBatchOp extends BaseSqlApiBatchOp<JoinBatchOp>
    implements JoinParams<JoinBatchOp> {

    enum Op implements Serializable {
        JOIN,
        LEFTOUTERJOIN,
        RIGHTOUTERJOIN,
        FULLOUTERJOIN;
    }

    public JoinBatchOp() {
        this(new Params());
    }

    public JoinBatchOp(String joinPredicate) {
        this(joinPredicate, "*");
    }

    public JoinBatchOp(String joinPredicate, String selectClause) {
        this(new Params()
            .set(JOIN_PREDICATE, joinPredicate)
            .set(SELECT_CLAUSE, selectClause));
    }

    public JoinBatchOp(Params params) {
        super(params);
    }

    @Override
    public JoinBatchOp linkFrom(BatchOperator<?>... inputs) {
        String selectClause = this.getParams().get(JoinParams.SELECT_CLAUSE);
        String joidPredicate = this.getParams().get(JoinParams.JOIN_PREDICATE);

        Op op = Op.valueOf(getType().toUpperCase());
        BatchOperator outputOp = null;
        switch (op) {
            case JOIN:
                outputOp = BatchSqlOperators.join(inputs[0], inputs[1], joidPredicate, selectClause);
                break;
            case LEFTOUTERJOIN:
                outputOp = BatchSqlOperators.leftOuterJoin(inputs[0], inputs[1], joidPredicate, selectClause);
                break;
            case RIGHTOUTERJOIN:
                outputOp = BatchSqlOperators.rightOuterJoin(inputs[0], inputs[1], joidPredicate, selectClause);
                break;
            case FULLOUTERJOIN:
                outputOp = BatchSqlOperators.fullOuterJoin(inputs[0], inputs[1], joidPredicate, selectClause);
                break;
            default:
                throw new RuntimeException("Not supported binary op: " + op);
        }
        this.setOutputTable(outputOp.getOutputTable());
        return this;
    }
}
