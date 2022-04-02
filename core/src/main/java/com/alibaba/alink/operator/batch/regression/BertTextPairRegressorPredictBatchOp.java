package com.alibaba.alink.operator.batch.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;

/**
 * Prediction with a text pair regressor using Bert models.
 */
@NameCn("Bert文本对回归预测")
public class BertTextPairRegressorPredictBatchOp extends
	TFTableModelRegressorPredictBatchOp <BertTextPairRegressorPredictBatchOp> {

	public BertTextPairRegressorPredictBatchOp() {
		this(new Params());
	}

	public BertTextPairRegressorPredictBatchOp(Params params) {
		super(params);
	}
}
