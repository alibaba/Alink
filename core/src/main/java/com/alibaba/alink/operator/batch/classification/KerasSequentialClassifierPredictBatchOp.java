package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;

/**
 * Prediction with a classifier using a Keras Sequential model.
 */
@NameCn("KerasSequential分类预测")
public class KerasSequentialClassifierPredictBatchOp
	extends TFTableModelClassifierPredictBatchOp <KerasSequentialClassifierPredictBatchOp> {

	public KerasSequentialClassifierPredictBatchOp() {
		this(new Params());
	}

	public KerasSequentialClassifierPredictBatchOp(Params params) {
		super(params);
	}
}
