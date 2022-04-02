package com.alibaba.alink.operator.batch.sql;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.sql.AsParams;

/**
 * Rename the fields of a batch operator.
 */
@NameCn("SQL操作：As")
public final class AsBatchOp extends BaseSqlApiBatchOp <AsBatchOp>
	implements AsParams <AsBatchOp> {

	private static final long serialVersionUID = -6266483708473673388L;

	public AsBatchOp() {
		this(new Params());
	}

	public AsBatchOp(String clause) {
		this(new Params().set(CLAUSE, clause));
	}

	public AsBatchOp(Params params) {
		super(params);
	}

	@Override
	public AsBatchOp linkFrom(BatchOperator <?>... inputs) {
		this.setOutputTable(inputs[0].as(getClause()).getOutputTable());
		return this;
	}
}
