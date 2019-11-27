package com.alibaba.alink.operator.batch.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.regression.IsotonicRegressionModelMapper;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.params.regression.IsotonicRegPredictParams;

/**
 * Isotonic Regression.
 * Implement parallelized pool adjacent violators algorithm.
 */
public final class IsotonicRegPredictBatchOp extends ModelMapBatchOp <IsotonicRegPredictBatchOp>
	implements IsotonicRegPredictParams <IsotonicRegPredictBatchOp> {

	/**
	 * Constructor.
	 */
	public IsotonicRegPredictBatchOp() {
		this(new Params());
	}

	/**
	 * Constructor.
	 * @param params the params of the algorithm.
	 */
	public IsotonicRegPredictBatchOp(Params params) {
		super(IsotonicRegressionModelMapper::new, params);
	}
}
