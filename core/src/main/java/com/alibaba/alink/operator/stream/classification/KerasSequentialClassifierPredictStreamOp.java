package com.alibaba.alink.operator.stream.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;

/**
 * Predict with a classifier using a Keras Sequential model.
 */
public class KerasSequentialClassifierPredictStreamOp
	extends TFTableModelClassifierPredictStreamOp <KerasSequentialClassifierPredictStreamOp> {

	public KerasSequentialClassifierPredictStreamOp(BatchOperator <?> model) {
		this(model, new Params());
	}

	public KerasSequentialClassifierPredictStreamOp(BatchOperator <?> model,
													Params params) {
		super(model, params);
	}
}
