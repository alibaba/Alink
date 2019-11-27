package com.alibaba.alink.pipeline.regression;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.regression.IsotonicRegTrainBatchOp;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.params.regression.IsotonicRegPredictParams;
import com.alibaba.alink.params.regression.IsotonicRegTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * Isotonic Regression.
 * Implement parallelized pool adjacent violators algorithm.
 * Support single feature input or vector input(extractor one index of the vector).
 */
public class IsotonicRegression extends Trainer <IsotonicRegression, IsotonicRegressionModel> implements
	IsotonicRegTrainParams <IsotonicRegression>,
	IsotonicRegPredictParams <IsotonicRegression> {

	public IsotonicRegression() {
		this(null);
	}

	public IsotonicRegression(Params params) {
		super(params);
	}

	@Override
	protected BatchOperator train(BatchOperator in) {
		return new IsotonicRegTrainBatchOp(this.getParams()).linkFrom(in);
	}
}

