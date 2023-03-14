package com.alibaba.alink.operator.stream.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.BatchOperator;

/**
 * Prediction with a text pair regressor using Bert models.
 */
@NameCn("Bert文本对分类预测")
@NameEn("Bert Text Pair Regressor Prediction")
public class BertTextPairRegressorPredictStreamOp extends
	TFTableModelRegressorPredictStreamOp <BertTextPairRegressorPredictStreamOp> {

	public BertTextPairRegressorPredictStreamOp() {
		super();
	}

	public BertTextPairRegressorPredictStreamOp(Params params) {
		super(params);
	}

	public BertTextPairRegressorPredictStreamOp(BatchOperator <?> model) {
		this(model, new Params());
	}

	public BertTextPairRegressorPredictStreamOp(BatchOperator <?> model, Params params) {
		super(model, params);
	}
}
