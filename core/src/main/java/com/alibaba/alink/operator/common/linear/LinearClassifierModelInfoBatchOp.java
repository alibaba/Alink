package com.alibaba.alink.operator.common.linear;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.lazy.ExtractModelInfoBatchOp;
import com.alibaba.alink.operator.batch.BatchOperator;

import java.util.List;

/**
 * Linear classifier model info batch op.
 */
public class LinearClassifierModelInfoBatchOp
	extends ExtractModelInfoBatchOp <LinearClassifierModelInfo, LinearClassifierModelInfoBatchOp> {

	private static final long serialVersionUID = -3598960496955727614L;

	public LinearClassifierModelInfoBatchOp() {
		this(null);
	}

	public LinearClassifierModelInfoBatchOp(Params params) {
		super(params);
	}

	@Override
	protected LinearClassifierModelInfo createModelInfo(List <Row> rows) {
		return new LinearClassifierModelInfo(rows);
	}

	@Override
	protected BatchOperator <?> processModel() {
		return this;
	}
}
