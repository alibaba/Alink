package com.alibaba.alink.operator.batch.sql;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.sql.FilterParams;

/**
 * Filter records in the batch operator.
 */
@NameCn("SQL操作：Filter")
@NameEn("SQL Filter Operation")
public final class FilterBatchOp extends BaseSqlApiBatchOp <FilterBatchOp>
	implements FilterParams <FilterBatchOp> {

	private static final long serialVersionUID = 1182682104232353734L;

	public FilterBatchOp() {
		this(new Params());
	}

	public FilterBatchOp(String clause) {
		this(new Params().set(CLAUSE, clause));
	}

	public FilterBatchOp(Params params) {
		super(params);
	}

	@Override
	public FilterBatchOp linkFrom(BatchOperator <?>... inputs) {
		this.setOutputTable(inputs[0].filter(getClause()).getOutputTable());
		return this;
	}
}
