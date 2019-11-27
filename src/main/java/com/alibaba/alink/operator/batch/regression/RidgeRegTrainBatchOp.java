package com.alibaba.alink.operator.batch.regression;

import com.alibaba.alink.operator.common.linear.BaseLinearModelTrainBatchOp;
import com.alibaba.alink.operator.common.linear.LinearModelType;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.params.regression.RidgeRegTrainParams;

/**
 * Train a regression model with L2-regularization.
 *
 */
public final class RidgeRegTrainBatchOp extends BaseLinearModelTrainBatchOp<RidgeRegTrainBatchOp>
	implements RidgeRegTrainParams <RidgeRegTrainBatchOp> {

	public RidgeRegTrainBatchOp() {
		this(new Params());
	}

	public RidgeRegTrainBatchOp(Params params) {
		super(params.clone(), LinearModelType.LinearReg, "Ridge Regression");
	}
}
