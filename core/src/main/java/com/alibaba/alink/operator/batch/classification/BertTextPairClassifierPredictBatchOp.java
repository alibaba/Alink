package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.ml.api.misc.param.Params;

public class BertTextPairClassifierPredictBatchOp extends TFTableModelClassifierPredictBatchOp <BertTextPairClassifierPredictBatchOp> {

	public BertTextPairClassifierPredictBatchOp() {
		this(new Params());
	}

	public BertTextPairClassifierPredictBatchOp(Params params) {
		super(params);
	}
}
