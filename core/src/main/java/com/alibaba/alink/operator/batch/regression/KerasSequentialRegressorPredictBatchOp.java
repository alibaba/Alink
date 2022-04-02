package com.alibaba.alink.operator.batch.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;

/**
 * Prediction with a regressor using a Keras Sequential model.
 */
@NameCn("KerasSequential回归预测")
public class KerasSequentialRegressorPredictBatchOp
	extends TFTableModelRegressorPredictBatchOp <KerasSequentialRegressorPredictBatchOp> {

	public KerasSequentialRegressorPredictBatchOp() {
		this(new Params());
	}

	public KerasSequentialRegressorPredictBatchOp(Params params) {
		super(params);
	}
}
