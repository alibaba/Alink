package com.alibaba.alink.operator.batch.regression;

import org.apache.flink.ml.api.misc.param.Params;

/**
 * Prediction with a text regressor using Bert models.
 */
public class BertTextRegressorPredictBatchOp extends
	TFTableModelRegressorPredictBatchOp <BertTextRegressorPredictBatchOp> {

	public BertTextRegressorPredictBatchOp() {
		this(new Params());
	}

	public BertTextRegressorPredictBatchOp(Params params) {
		super(params);
	}
}
