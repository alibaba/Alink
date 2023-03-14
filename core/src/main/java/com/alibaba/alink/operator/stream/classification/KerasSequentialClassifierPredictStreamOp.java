package com.alibaba.alink.operator.stream.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.BatchOperator;

/**
 * Predict with a classifier using a Keras Sequential model.
 */
@NameCn("KerasSequential分类预测")
@NameEn("Keras Sequential Prediction")
public class KerasSequentialClassifierPredictStreamOp
	extends TFTableModelClassifierPredictStreamOp <KerasSequentialClassifierPredictStreamOp> {

	public KerasSequentialClassifierPredictStreamOp() {
		super();
	}

	public KerasSequentialClassifierPredictStreamOp(Params params) {
		super(params);
	}

	public KerasSequentialClassifierPredictStreamOp(BatchOperator <?> model) {
		this(model, new Params());
	}

	public KerasSequentialClassifierPredictStreamOp(BatchOperator <?> model,
													Params params) {
		super(model, params);
	}
}
