package com.alibaba.alink.operator.batch.sql;

import com.alibaba.alink.operator.batch.BatchOperator;
import org.apache.flink.ml.api.misc.param.Params;

/**
 * The base class of all batch sql operators.
 * <p>
 * The batch sql operators apply the sql operation (select, where, group by, join, etc.) on their
 * input batch operators.
 */
public abstract class BaseSqlApiBatchOp<T extends BaseSqlApiBatchOp<T>> extends BatchOperator<T> {

    public BaseSqlApiBatchOp(Params params) {
        super(params);
    }
}
