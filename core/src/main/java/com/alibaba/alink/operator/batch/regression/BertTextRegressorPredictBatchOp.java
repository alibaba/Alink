package com.alibaba.alink.operator.batch.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;

/**
 * Prediction with a text regressor using Bert models.
 */
@NameCn("Bert文本回归预测")
@NameEn("Bert Text Regression Prediction")
public class BertTextRegressorPredictBatchOp extends
	TFTableModelRegressorPredictBatchOp <BertTextRegressorPredictBatchOp> {

	public BertTextRegressorPredictBatchOp() {
		this(new Params());
	}

	public BertTextRegressorPredictBatchOp(Params params) {
		super(params);
	}
}
