package com.alibaba.alink.operator.local.sql;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.params.sql.FilterParams;

/**
 * Filter records in the batch operator.
 */
@NameCn("SQL操作：Filter")
public final class FilterLocalOp extends BaseSqlApiLocalOp <FilterLocalOp>
	implements FilterParams <FilterLocalOp> {

	private static final long serialVersionUID = 1182682104232353734L;

	public FilterLocalOp() {
		this(new Params());
	}

	public FilterLocalOp(String clause) {
		this(new Params().set(CLAUSE, clause));
	}

	public FilterLocalOp(Params params) {
		super(params);
	}

	@Override
	public FilterLocalOp linkFrom(LocalOperator <?>... inputs) {
		this.setOutputTable(inputs[0].filter(getClause()).getOutputTable());
		return this;
	}
}
