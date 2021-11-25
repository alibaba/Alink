package com.alibaba.alink.operator.batch.regression;

import org.apache.flink.ml.api.misc.param.Params;

/**
 * Prediction with a text pair regressor using Bert models.
 */
public class BertTextPairRegressorPredictBatchOp extends
	TFTableModelRegressorPredictBatchOp <BertTextPairRegressorPredictBatchOp> {

	public BertTextPairRegressorPredictBatchOp() {
		this(new Params());
	}

	public BertTextPairRegressorPredictBatchOp(Params params) {
		super(params);
	}
}
