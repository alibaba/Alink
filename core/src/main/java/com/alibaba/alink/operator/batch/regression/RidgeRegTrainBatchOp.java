package com.alibaba.alink.operator.batch.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.lazy.WithModelInfoBatchOp;
import com.alibaba.alink.operator.common.linear.BaseLinearModelTrainBatchOp;
import com.alibaba.alink.operator.common.linear.LinearModelType;
import com.alibaba.alink.operator.common.linear.LinearRegressorModelInfo;
import com.alibaba.alink.params.regression.RidgeRegTrainParams;

/**
 * Train a regression model with L2-regularization.
 */
public final class RidgeRegTrainBatchOp extends BaseLinearModelTrainBatchOp <RidgeRegTrainBatchOp>
	implements RidgeRegTrainParams <RidgeRegTrainBatchOp>,
	WithModelInfoBatchOp <LinearRegressorModelInfo, RidgeRegTrainBatchOp, RidgeRegModelInfoBatchOp> {

	private static final long serialVersionUID = -1939712619795581386L;

	public RidgeRegTrainBatchOp() {
		this(new Params());
	}

	public RidgeRegTrainBatchOp(Params params) {
		super(params.clone(), LinearModelType.LinearReg, "Ridge Regression");
	}

	@Override
	public RidgeRegModelInfoBatchOp getModelInfoBatchOp() {
		return new RidgeRegModelInfoBatchOp(this.getParams()).linkFrom(this);
	}

}
