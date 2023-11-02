package com.alibaba.alink.operator.stream.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.linear.LinearModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.regression.LinearRegStepwisePredictParams;
import com.alibaba.alink.params.shared.HasNumThreads;

/**
 * *
 *
 * @author weibo zhao
 */
@NameCn("线性回归Stepwise流预测")
@NameEn("Linear Regresion Stepwise Prediction")
public class LinearRegStepwisePredictStreamOp extends ModelMapStreamOp <LinearRegStepwisePredictStreamOp>
	implements LinearRegStepwisePredictParams <LinearRegStepwisePredictStreamOp> {

	private static final long serialVersionUID = 471475458545672230L;

	public LinearRegStepwisePredictStreamOp() {
		super(LinearModelMapper::new, new Params());
	}

	public LinearRegStepwisePredictStreamOp(Params params) {
		super(LinearModelMapper::new, params);
	}

	public LinearRegStepwisePredictStreamOp(BatchOperator model) {
		super(model, LinearModelMapper::new, new Params());
	}

	public LinearRegStepwisePredictStreamOp(BatchOperator model, Params params) {
		super(model, LinearModelMapper::new, params);
	}

}
