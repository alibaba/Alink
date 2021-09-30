package com.alibaba.alink.operator.stream.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;

public class BertTextPairClassifierPredictStreamOp extends
	TFTableModelClassifierPredictStreamOp <BertTextPairClassifierPredictStreamOp> {

	public BertTextPairClassifierPredictStreamOp(BatchOperator <?> model) {
		this(model, new Params());
	}

	public BertTextPairClassifierPredictStreamOp(BatchOperator <?> model, Params params) {
		super(model, params);
	}
}
