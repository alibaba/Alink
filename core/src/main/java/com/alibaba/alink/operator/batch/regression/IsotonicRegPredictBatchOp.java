package com.alibaba.alink.operator.batch.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.regression.IsotonicRegressionModelMapper;
import com.alibaba.alink.params.regression.IsotonicRegPredictParams;

/**
 * Isotonic Regression.
 * Implement parallelized pool adjacent violators algorithm.
 */
@NameCn("保序回归预测")
@NameEn("Isotonic Regression Prediction")
public final class IsotonicRegPredictBatchOp extends ModelMapBatchOp <IsotonicRegPredictBatchOp>
	implements IsotonicRegPredictParams <IsotonicRegPredictBatchOp> {

	private static final long serialVersionUID = 7021808598401143397L;

	/**
	 * Constructor.
	 */
	public IsotonicRegPredictBatchOp() {
		this(new Params());
	}

	/**
	 * Constructor.
	 *
	 * @param params the params of the algorithm.
	 */
	public IsotonicRegPredictBatchOp(Params params) {
		super(IsotonicRegressionModelMapper::new, params);
	}
}
