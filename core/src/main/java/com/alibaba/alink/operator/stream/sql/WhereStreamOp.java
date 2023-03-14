package com.alibaba.alink.operator.stream.sql;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.sql.WhereParams;

/**
 * Filter records in the stream operator.
 */
@NameCn("SQL操作：Where")
@NameEn("SQL：Where")
public final class WhereStreamOp extends BaseSqlApiStreamOp <WhereStreamOp>
	implements WhereParams <WhereStreamOp> {

	private static final long serialVersionUID = -353238089567068370L;

	public WhereStreamOp() {
		this(new Params());
	}

	public WhereStreamOp(String clause) {
		this(new Params().set(CLAUSE, clause));
	}

	public WhereStreamOp(Params params) {
		super(params);
	}

	@Override
	public WhereStreamOp linkFrom(StreamOperator <?>... inputs) {
		this.setOutputTable(inputs[0].where(getClause()).getOutputTable());
		return this;
	}
}
