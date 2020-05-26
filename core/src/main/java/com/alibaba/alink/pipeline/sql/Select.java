package com.alibaba.alink.pipeline.sql;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.sql.SelectBatchOp;
import com.alibaba.alink.operator.common.sql.SelectMapper;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.sql.SelectStreamOp;
import com.alibaba.alink.params.sql.SelectParams;
import com.alibaba.alink.pipeline.MapTransformer;

/**
 * Execute sql select statement.
 */
public class Select extends MapTransformer <Select>
	implements SelectParams<Select> {

	public Select() {
		this(null);
	}

	public Select(Params params) {
		super(SelectMapper::new, params);
	}

	@Override
	public BatchOperator transform(BatchOperator input) {
		return new SelectBatchOp().setClause(this.getClause()).linkFrom(input);
	}

	@Override
	public StreamOperator transform(StreamOperator input) {
		return new SelectStreamOp().setClause(this.getClause()).linkFrom(input);
	}
}
