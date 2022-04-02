package com.alibaba.alink.operator.batch.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.lazy.WithModelInfoBatchOp;
import com.alibaba.alink.operator.common.linear.BaseLinearModelTrainBatchOp;
import com.alibaba.alink.operator.common.linear.LinearModelType;
import com.alibaba.alink.operator.common.linear.LinearRegressorModelInfo;
import com.alibaba.alink.params.regression.LinearRegTrainParams;

/**
 * Train a regression model.
 */
@NameCn("线性回归训练")
public final class LinearRegTrainBatchOp extends BaseLinearModelTrainBatchOp <LinearRegTrainBatchOp>
	implements LinearRegTrainParams <LinearRegTrainBatchOp>,
	WithModelInfoBatchOp <LinearRegressorModelInfo, LinearRegTrainBatchOp, LinearRegModelInfoBatchOp> {

	private static final long serialVersionUID = -8737435600011807472L;

	public LinearRegTrainBatchOp() {
		this(new Params());
	}

	public LinearRegTrainBatchOp(Params params) {
		super(params.clone(), LinearModelType.LinearReg, "Linear Regression");
	}

	@Override
	public LinearRegModelInfoBatchOp getModelInfoBatchOp() {
		return new LinearRegModelInfoBatchOp(this.getParams()).linkFrom(this);
	}
}
