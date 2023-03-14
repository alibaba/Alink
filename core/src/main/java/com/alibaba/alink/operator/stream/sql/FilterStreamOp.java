package com.alibaba.alink.operator.stream.sql;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.sql.FilterParams;

/**
 * Filter records in the stream operator.
 */
@NameCn("SQL操作：Filter")
@NameEn("SQL：Filter")
public final class FilterStreamOp extends BaseSqlApiStreamOp <FilterStreamOp>
	implements FilterParams <FilterStreamOp> {

	private static final long serialVersionUID = 6731449495440163599L;

	public FilterStreamOp() {
		this(new Params());
	}

	public FilterStreamOp(String clause) {
		this(new Params().set(CLAUSE, clause));
	}

	public FilterStreamOp(Params params) {
		super(params);
	}

	@Override
	public FilterStreamOp linkFrom(StreamOperator <?>... inputs) {
		this.setOutputTable(inputs[0].filter(getClause()).getOutputTable());
		return this;
	}
}
