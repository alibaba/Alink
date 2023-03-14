package com.alibaba.alink.operator.batch.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;

/**
 * Prediction with a regressor using a Keras Sequential model.
 */
@NameCn("KerasSequential回归预测")
@NameEn("KerasSequential Regression Prediction")
public class KerasSequentialRegressorPredictBatchOp
	extends TFTableModelRegressorPredictBatchOp <KerasSequentialRegressorPredictBatchOp> {

	public KerasSequentialRegressorPredictBatchOp() {
		this(new Params());
	}

	public KerasSequentialRegressorPredictBatchOp(Params params) {
		super(params);
	}
}
