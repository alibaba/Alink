package com.alibaba.alink.operator.batch.regression;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.lazy.ExtractModelInfoBatchOp;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.linear.LinearRegressorModelInfo;

import java.util.List;

public class LinearRegModelInfoBatchOp
	extends ExtractModelInfoBatchOp <LinearRegressorModelInfo, LinearRegModelInfoBatchOp> {

	private static final long serialVersionUID = 8720107522407961502L;

	public LinearRegModelInfoBatchOp() {
		this(null);
	}

	public LinearRegModelInfoBatchOp(Params params) {
		super(params);
	}

	@Override
	protected LinearRegressorModelInfo createModelInfo(List <Row> rows) {
		return new LinearRegressorModelInfo(rows);
	}

	@Override
	protected BatchOperator <?> processModel() {
		return this;
	}
}
