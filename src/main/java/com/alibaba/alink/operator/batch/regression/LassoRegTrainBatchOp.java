package com.alibaba.alink.operator.batch.regression;

import com.alibaba.alink.operator.common.linear.BaseLinearModelTrainBatchOp;
import com.alibaba.alink.operator.common.linear.LinearModelType;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.params.regression.LassoRegTrainParams;

/**
 * Train a regression model with L1-regularization.
 *
 */
public final class LassoRegTrainBatchOp extends BaseLinearModelTrainBatchOp<LassoRegTrainBatchOp>
	implements LassoRegTrainParams <LassoRegTrainBatchOp> {

	public LassoRegTrainBatchOp() {
		this(new Params());
	}

	public LassoRegTrainBatchOp(Params params) {
		super(params.clone(), LinearModelType.LinearReg, "LASSO");
	}
}
