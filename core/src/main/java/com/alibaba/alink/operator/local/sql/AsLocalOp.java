package com.alibaba.alink.operator.local.sql;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.params.sql.AsParams;

/**
 * Rename the fields of a batch operator.
 */
@NameCn("SQL操作：As")
public final class AsLocalOp extends BaseSqlApiLocalOp <AsLocalOp>
	implements AsParams <AsLocalOp> {

	private static final long serialVersionUID = -6266483708473673388L;

	public AsLocalOp() {
		this(new Params());
	}

	public AsLocalOp(String clause) {
		this(new Params().set(CLAUSE, clause));
	}

	public AsLocalOp(Params params) {
		super(params);
	}

	@Override
	protected void linkFromImpl(LocalOperator <?>... inputs) {
		this.setOutputTable(inputs[0].as(getClause()).getOutputTable());
	}
}
