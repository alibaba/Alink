package com.alibaba.alink.operator.local.sql;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.params.dataproc.HasClause;
import com.alibaba.alink.params.sql.WhereParams;

/**
 * Filter records in the batch operator.
 */
@NameCn("SQL操作：Where")
public final class WhereLocalOp extends BaseSqlApiLocalOp <WhereLocalOp>
	implements WhereParams <WhereLocalOp> {

	private static final long serialVersionUID = 2425170045693249109L;

	public WhereLocalOp() {
		this(new Params());
	}

	public WhereLocalOp(String clause) {
		this(new Params().set(HasClause.CLAUSE, clause));
	}

	public WhereLocalOp(Params params) {
		super(params);
	}

	@Override
	protected void linkFromImpl(LocalOperator <?>... inputs) {
		this.setOutputTable(inputs[0].where(getClause()).getOutputTable());
	}
}
