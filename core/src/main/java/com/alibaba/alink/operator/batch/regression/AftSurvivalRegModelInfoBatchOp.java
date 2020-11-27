package com.alibaba.alink.operator.batch.regression;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.lazy.ExtractModelInfoBatchOp;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.linear.LinearRegressorModelInfo;

import java.util.List;

public class AftSurvivalRegModelInfoBatchOp
	extends ExtractModelInfoBatchOp <LinearRegressorModelInfo, AftSurvivalRegModelInfoBatchOp> {

	private static final long serialVersionUID = -6418252731236295797L;

	public AftSurvivalRegModelInfoBatchOp() {
		this(null);
	}

	public AftSurvivalRegModelInfoBatchOp(Params params) {
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
