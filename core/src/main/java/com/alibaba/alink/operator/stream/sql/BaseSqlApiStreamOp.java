package com.alibaba.alink.operator.stream.sql;

import com.alibaba.alink.operator.stream.StreamOperator;
import org.apache.flink.ml.api.misc.param.Params;

/**
 * The base class of all stream sql operators.
 * <p>
 * The stream sql operators apply the sql operation (select, where, filter, as) on their
 * input stream operators.
 */
public abstract class BaseSqlApiStreamOp<T extends BaseSqlApiStreamOp<T>> extends StreamOperator<T> {
    BaseSqlApiStreamOp(Params params) {
        super(params);
    }
}
