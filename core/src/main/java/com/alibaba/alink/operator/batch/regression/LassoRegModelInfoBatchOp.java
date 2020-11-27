package com.alibaba.alink.operator.batch.regression;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.lazy.ExtractModelInfoBatchOp;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.linear.LinearRegressorModelInfo;

import java.util.List;

public class LassoRegModelInfoBatchOp
	extends ExtractModelInfoBatchOp <LinearRegressorModelInfo, LassoRegModelInfoBatchOp> {

	private static final long serialVersionUID = -4827765586949128150L;

	public LassoRegModelInfoBatchOp() {
		this(null);
	}

	public LassoRegModelInfoBatchOp(Params params) {
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
