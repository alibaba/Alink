package com.alibaba.alink.operator.local.sql;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.params.sql.OrderByParams;

/**
 * Order the batch operator.
 */
@NameCn("SQL操作：OrderBy")
public final class OrderByLocalOp extends BaseSqlApiLocalOp <OrderByLocalOp>
	implements OrderByParams <OrderByLocalOp> {

	private static final long serialVersionUID = -8600279903752321912L;

	public OrderByLocalOp() {
		this(new Params());
	}

	public OrderByLocalOp(Params params) {
		super(params);
	}

	private int getOrderByParamWithDefault(ParamInfo <Integer> paramInfo) {
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
	protected void linkFromImpl(LocalOperator <?>... inputs) {
		LocalOperator <?> outputOp;
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
	}
}
