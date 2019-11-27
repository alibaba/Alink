package com.alibaba.alink.operator.batch.regression;

import com.alibaba.alink.operator.common.linear.BaseLinearModelTrainBatchOp;
import com.alibaba.alink.operator.common.linear.LinearModelType;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.params.regression.LinearRegTrainParams;

/**
 * Train a regression model.
 *
 */
public final class LinearRegTrainBatchOp extends BaseLinearModelTrainBatchOp<LinearRegTrainBatchOp>
	implements LinearRegTrainParams <LinearRegTrainBatchOp> {

	public LinearRegTrainBatchOp() {
		this(new Params());
	}

	public LinearRegTrainBatchOp(Params params) {
		super(params.clone(), LinearModelType.LinearReg, "Linear Regression");
	}

}
