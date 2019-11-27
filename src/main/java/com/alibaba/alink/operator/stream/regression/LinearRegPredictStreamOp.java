package com.alibaba.alink.operator.stream.regression;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.linear.LinearModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.regression.LinearRegPredictParams;

import org.apache.flink.ml.api.misc.param.Params;

/**
 * Linear regression predict stream operator. this operator predict data's regression value with linear model.
 *
 */
public class LinearRegPredictStreamOp extends ModelMapStreamOp <LinearRegPredictStreamOp>
	implements LinearRegPredictParams <LinearRegPredictStreamOp> {

	public LinearRegPredictStreamOp(BatchOperator model) {
		super(model, LinearModelMapper::new, new Params());
	}

	public LinearRegPredictStreamOp(BatchOperator model, Params params) {
		super(model, LinearModelMapper::new, params);
	}

}
