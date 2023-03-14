package com.alibaba.alink.operator.batch.sql;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.dataproc.HasClause;
import com.alibaba.alink.params.sql.WhereParams;

/**
 * Filter records in the batch operator.
 */
@NameCn("SQL操作：Where")
@NameEn("SQL Where Operation")
public final class WhereBatchOp extends BaseSqlApiBatchOp <WhereBatchOp>
	implements WhereParams <WhereBatchOp> {

	private static final long serialVersionUID = 2425170045693249109L;

	public WhereBatchOp() {
		this(new Params());
	}

	public WhereBatchOp(String clause) {
		this(new Params().set(HasClause.CLAUSE, clause));
	}

	public WhereBatchOp(Params params) {
		super(params);
	}

	@Override
	public WhereBatchOp linkFrom(BatchOperator <?>... inputs) {
		this.setOutputTable(inputs[0].where(getClause()).getOutputTable());
		return this;
	}
}
