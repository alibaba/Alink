package com.alibaba.alink.operator.stream.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.regression.IsotonicRegressionModelMapper;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.regression.IsotonicRegPredictParams;

/**
 * Isotonic Regression.
 * Implement parallelized pool adjacent violators algorithm.
 * Support single feature input or vector input(extractor one index of the vector).
 */
public class IsotonicRegPredictStreamOp extends ModelMapStreamOp <IsotonicRegPredictStreamOp>
	implements IsotonicRegPredictParams <IsotonicRegPredictStreamOp> {
	public IsotonicRegPredictStreamOp(BatchOperator model) {
		this(model, new Params());
	}

	public IsotonicRegPredictStreamOp(BatchOperator model, Params params) {
		super(model, IsotonicRegressionModelMapper::new, params);
	}
}
