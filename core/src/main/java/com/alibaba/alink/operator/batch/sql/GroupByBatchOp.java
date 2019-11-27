package com.alibaba.alink.operator.batch.sql;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.sql.GroupByParams;

/**
 * Apply the "group by" operation on the input batch operator.
 */
public final class GroupByBatchOp extends BaseSqlApiBatchOp<GroupByBatchOp>
    implements GroupByParams<GroupByBatchOp> {

    public GroupByBatchOp() {
        this(new Params());
    }

    public GroupByBatchOp(String groupByClause, String selectClause) {
        this(new Params().set(GroupByParams.SELECT_CLAUSE, selectClause)
            .set(GroupByParams.GROUP_BY_PREDICATE, groupByClause));
    }

    public GroupByBatchOp(Params params) {
        super(params);
    }

    @Override
    public GroupByBatchOp linkFrom(BatchOperator<?>... inputs) {
        this.setOutputTable(inputs[0].groupBy(getGroupByPredicate(), getSelectClause()).getOutputTable());
        return this;
    }
}
