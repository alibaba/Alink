package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.ml.api.misc.param.Params;

public class BertTextClassifierPredictBatchOp extends TFTableModelClassifierPredictBatchOp <BertTextClassifierPredictBatchOp> {

	public BertTextClassifierPredictBatchOp() {
		this(new Params());
	}

	public BertTextClassifierPredictBatchOp(Params params) {
		super(params);
	}
}
