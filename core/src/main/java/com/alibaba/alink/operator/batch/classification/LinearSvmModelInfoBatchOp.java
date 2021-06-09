package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.lazy.ExtractModelInfoBatchOp;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.linear.LinearClassifierModelInfo;

import java.util.List;

/**
 * Linear classifier model info batch op.
 */
public class LinearSvmModelInfoBatchOp
	extends ExtractModelInfoBatchOp <LinearClassifierModelInfo, LinearSvmModelInfoBatchOp> {

	private static final long serialVersionUID = -3598960496955727614L;

	public LinearSvmModelInfoBatchOp() {
		this(null);
	}

	public LinearSvmModelInfoBatchOp(Params params) {
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
