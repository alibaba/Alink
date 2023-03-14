package com.alibaba.alink.operator.stream.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.BatchOperator;

/**
 * Prediction with a regressor using a Keras Sequential model.
 */
@NameCn("KerasSequential回归预测")
@NameEn("KerasSequential Regression Prediction")
public class KerasSequentialRegressorPredictStreamOp
	extends TFTableModelRegressorPredictStreamOp <KerasSequentialRegressorPredictStreamOp> {

	public KerasSequentialRegressorPredictStreamOp() {
		super();
	}

	public KerasSequentialRegressorPredictStreamOp(Params params) {
		super(params);
	}

	public KerasSequentialRegressorPredictStreamOp(BatchOperator <?> model) {
		this(model, new Params());
	}

	public KerasSequentialRegressorPredictStreamOp(BatchOperator <?> model,
												   Params params) {
		super(model, params);
	}
}
