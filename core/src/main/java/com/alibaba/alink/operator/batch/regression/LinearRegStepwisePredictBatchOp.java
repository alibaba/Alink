package com.alibaba.alink.operator.batch.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.linear.LinearModelMapper;
import com.alibaba.alink.params.regression.LinearRegStepwisePredictParams;

/**
 * *
 *
 * @author weibo zhao
 */
@NameCn("线性回归Stepwise预测")
@NameEn("Stepwise Linear Regression Prediction")
public final class LinearRegStepwisePredictBatchOp extends ModelMapBatchOp <LinearRegStepwisePredictBatchOp>
	implements LinearRegStepwisePredictParams <LinearRegStepwisePredictBatchOp> {

	private static final long serialVersionUID = -4433560932329911993L;

	public LinearRegStepwisePredictBatchOp() {
		this(new Params());
	}

	public LinearRegStepwisePredictBatchOp(Params params) {
		super(LinearModelMapper::new, params);
	}
}
