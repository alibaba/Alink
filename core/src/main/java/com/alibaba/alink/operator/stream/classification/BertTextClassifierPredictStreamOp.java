package com.alibaba.alink.operator.stream.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;

/**
 * Predict with a text classifier using Bert models.
 */
public class BertTextClassifierPredictStreamOp extends
	TFTableModelClassifierPredictStreamOp <BertTextClassifierPredictStreamOp> {

	public BertTextClassifierPredictStreamOp(BatchOperator <?> model) {
		this(model, new Params());
	}

	public BertTextClassifierPredictStreamOp(BatchOperator <?> model, Params params) {
		super(model, params);
	}
}
