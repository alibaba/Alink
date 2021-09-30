package com.alibaba.alink.operator.stream.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;

public class BertTextClassifierPredictStreamOp extends
	TFTableModelClassifierPredictStreamOp <BertTextClassifierPredictStreamOp> {

	public BertTextClassifierPredictStreamOp(BatchOperator <?> model) {
		this(model, new Params());
	}

	public BertTextClassifierPredictStreamOp(BatchOperator <?> model, Params params) {
		super(model, params);
	}
}
