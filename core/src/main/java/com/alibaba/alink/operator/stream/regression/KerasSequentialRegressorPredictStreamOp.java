package com.alibaba.alink.operator.stream.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;

/**
 * Prediction with a regressor using a Keras Sequential model.
 */
public class KerasSequentialRegressorPredictStreamOp
	extends TFTableModelRegressorPredictStreamOp <KerasSequentialRegressorPredictStreamOp> {

	public KerasSequentialRegressorPredictStreamOp(BatchOperator <?> model) {
		this(model, new Params());
	}

	public KerasSequentialRegressorPredictStreamOp(BatchOperator <?> model,
												   Params params) {
		super(model, params);
	}
}
