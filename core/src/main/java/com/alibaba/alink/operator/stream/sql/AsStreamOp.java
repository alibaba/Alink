package com.alibaba.alink.operator.stream.sql;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.sql.AsParams;

/**
 * Rename the fields of a stream operator.
 */
@NameCn("SQL操作：As")
public final class AsStreamOp extends BaseSqlApiStreamOp <AsStreamOp>
	implements AsParams <AsStreamOp> {

	private static final long serialVersionUID = -7230853155177883256L;

	public AsStreamOp() {
		this(new Params());
	}

	public AsStreamOp(String clause) {
		this(new Params().set(CLAUSE, clause));
	}

	public AsStreamOp(Params params) {
		super(params);
	}

	@Override
	public AsStreamOp linkFrom(StreamOperator <?>... inputs) {
		this.setOutputTable(inputs[0].as(getClause()).getOutputTable());
		return this;
	}
}
