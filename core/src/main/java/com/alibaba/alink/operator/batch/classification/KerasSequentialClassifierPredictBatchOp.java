package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.ml.api.misc.param.Params;

/**
 * Prediction with a classifier using a Keras Sequential model.
 */
public class KerasSequentialClassifierPredictBatchOp
	extends TFTableModelClassifierPredictBatchOp <KerasSequentialClassifierPredictBatchOp> {

	public KerasSequentialClassifierPredictBatchOp() {
		this(new Params());
	}

	public KerasSequentialClassifierPredictBatchOp(Params params) {
		super(params);
	}
}
