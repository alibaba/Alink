package com.alibaba.alink.operator.batch.regression;

import org.apache.flink.ml.api.misc.param.Params;

/**
 * Prediction with a regressor using a Keras Sequential model.
 */
public class KerasSequentialRegressorPredictBatchOp
	extends TFTableModelRegressorPredictBatchOp <KerasSequentialRegressorPredictBatchOp> {

	public KerasSequentialRegressorPredictBatchOp() {
		this(new Params());
	}

	public KerasSequentialRegressorPredictBatchOp(Params params) {
		super(params);
	}
}
