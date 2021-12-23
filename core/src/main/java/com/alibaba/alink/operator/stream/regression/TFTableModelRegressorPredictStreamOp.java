package com.alibaba.alink.operator.stream.regression;

import org.apache.flink.annotation.Internal;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.regression.tensorflow.TFTableModelRegressionModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.regression.TFTableModelRegressionPredictParams;

@Internal
public class TFTableModelRegressorPredictStreamOp<T extends TFTableModelRegressorPredictStreamOp <T>>
	extends ModelMapStreamOp <T> implements TFTableModelRegressionPredictParams <T> {

	public TFTableModelRegressorPredictStreamOp(BatchOperator model) {
		this(model, new Params());
	}

	public TFTableModelRegressorPredictStreamOp(BatchOperator model, Params params) {
		super(model, TFTableModelRegressionModelMapper::new, params);
	}
}
