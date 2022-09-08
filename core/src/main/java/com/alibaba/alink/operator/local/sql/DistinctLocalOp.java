package com.alibaba.alink.operator.local.sql;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.local.LocalOperator;

/**
 * Remove duplicated records.
 */
@NameCn("SQL操作：Distinct")
public final class DistinctLocalOp extends BaseSqlApiLocalOp <DistinctLocalOp> {

	private static final long serialVersionUID = 2774293287356122519L;

	public DistinctLocalOp() {
		this(new Params());
	}

	public DistinctLocalOp(Params params) {
		super(params);
	}

	@Override
	public DistinctLocalOp linkFrom(LocalOperator <?>... inputs) {
		this.setOutputTable(inputs[0].distinct().getOutputTable());
		return this;
	}
}
