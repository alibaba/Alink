package com.alibaba.alink.operator.batch.regression;

import org.apache.flink.ml.api.misc.param.Params;

public class KerasSequentialRegressorPredictBatchOp
	extends TFTableModelRegressorPredictBatchOp <KerasSequentialRegressorPredictBatchOp> {

	public KerasSequentialRegressorPredictBatchOp() {
		this(new Params());
	}

	public KerasSequentialRegressorPredictBatchOp(Params params) {
		super(params);
	}
}
