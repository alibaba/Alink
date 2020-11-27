package com.alibaba.alink.operator.stream.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.linear.LinearModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.regression.LinearRegPredictParams;

/**
 * Linear regression predict stream operator. this operator predict data's regression value with linear model.
 */
public class LinearRegPredictStreamOp extends ModelMapStreamOp <LinearRegPredictStreamOp>
	implements LinearRegPredictParams <LinearRegPredictStreamOp> {

	private static final long serialVersionUID = -5279929521940820773L;

	public LinearRegPredictStreamOp(BatchOperator model) {
		super(model, LinearModelMapper::new, new Params());
	}

	public LinearRegPredictStreamOp(BatchOperator model, Params params) {
		super(model, LinearModelMapper::new, params);
	}

}
