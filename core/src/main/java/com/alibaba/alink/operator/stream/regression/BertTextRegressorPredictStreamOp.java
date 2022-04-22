package com.alibaba.alink.operator.stream.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.batch.BatchOperator;

/**
 * Prediction with a text regressor using Bert models.
 */
@NameCn("Bert文本回归预测")
public class BertTextRegressorPredictStreamOp extends
	TFTableModelRegressorPredictStreamOp <BertTextRegressorPredictStreamOp> {

	public BertTextRegressorPredictStreamOp() {
		super();
	}

	public BertTextRegressorPredictStreamOp(Params params) {
		super(params);
	}

	public BertTextRegressorPredictStreamOp(BatchOperator <?> model) {
		this(model, new Params());
	}

	public BertTextRegressorPredictStreamOp(BatchOperator <?> model, Params params) {
		super(model, params);
	}
}
