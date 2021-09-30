package com.alibaba.alink.operator.stream.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;

public class BertTextRegressorPredictStreamOp extends
	TFTableModelRegressorPredictStreamOp <BertTextRegressorPredictStreamOp> {

	public BertTextRegressorPredictStreamOp(BatchOperator <?> model) {
		this(model, new Params());
	}

	public BertTextRegressorPredictStreamOp(BatchOperator <?> model, Params params) {
		super(model, params);
	}
}
