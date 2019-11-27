package com.alibaba.alink.operator.batch.sql;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.sql.OrderByParams;

/**
 * Order the batch operator.
 */
public final class OrderByBatchOp extends BaseSqlApiBatchOp<OrderByBatchOp>
    implements OrderByParams<OrderByBatchOp> {

    public OrderByBatchOp() {
        this(new Params());
    }

    public OrderByBatchOp(Params params) {
        super(params);
    }

    private int getOrderByParamWithDefault(ParamInfo<Integer> paramInfo) {
        int value = -1;
        if (this.getParams().contains(paramInfo)) {
            Integer v = this.getParams().get(paramInfo);
            if (v != null) {
                value = v;
            }
        }
        return value;
    }

    @Override
    public OrderByBatchOp linkFrom(BatchOperator<?>... inputs) {
        BatchOperator outputOp;
        int limit = getOrderByParamWithDefault(OrderByParams.LIMIT);
        boolean isAscending = getOrder().equalsIgnoreCase("asc");
        if (limit >= 0) {
            outputOp = inputs[0].orderBy(getClause(), limit, isAscending);
        } else {
            int offset = getOrderByParamWithDefault(OrderByParams.OFFSET);
            int fetch = getOrderByParamWithDefault(OrderByParams.FETCH);
            outputOp = inputs[0].orderBy(getClause(), offset, fetch, isAscending);
        }
        this.setOutputTable(outputOp.getOutputTable());
        return this;
    }
}
