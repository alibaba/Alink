package com.alibaba.alink.operator.stream.sql;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.stream.StreamOperator;

/**
 * The base class of all stream sql operators.
 * <p>
 * The stream sql operators apply the sql operation (select, where, filter, as) on their
 * input stream operators.
 */
public abstract class BaseSqlApiStreamOp<T extends BaseSqlApiStreamOp <T>> extends StreamOperator <T> {
	private static final long serialVersionUID = -2592914717165487285L;

	BaseSqlApiStreamOp(Params params) {
		super(params);
	}
}
