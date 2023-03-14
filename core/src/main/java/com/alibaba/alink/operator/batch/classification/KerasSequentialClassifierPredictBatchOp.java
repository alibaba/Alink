package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;

/**
 * Prediction with a classifier using a Keras Sequential model.
 */
@NameCn("KerasSequential分类预测")
@NameEn("KerasSequential Classifier Prediction")
public class KerasSequentialClassifierPredictBatchOp
	extends TFTableModelClassifierPredictBatchOp <KerasSequentialClassifierPredictBatchOp> {

	public KerasSequentialClassifierPredictBatchOp() {
		this(new Params());
	}

	public KerasSequentialClassifierPredictBatchOp(Params params) {
		super(params);
	}
}
