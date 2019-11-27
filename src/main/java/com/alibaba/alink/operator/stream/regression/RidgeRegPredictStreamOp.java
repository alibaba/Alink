package com.alibaba.alink.operator.stream.regression;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.linear.LinearModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.regression.RidgeRegPredictParams;

import org.apache.flink.ml.api.misc.param.Params;

/**
 * Ridge regression predict stream operator. this operator predict data's regression value with linear model.
 *
 */
public class RidgeRegPredictStreamOp extends ModelMapStreamOp <RidgeRegPredictStreamOp>
	implements RidgeRegPredictParams <RidgeRegPredictStreamOp> {

	public RidgeRegPredictStreamOp(BatchOperator model) {
		super(model, LinearModelMapper::new, new Params());
	}

	public RidgeRegPredictStreamOp(BatchOperator model, Params params) {
		super(model, LinearModelMapper::new, params);
	}

}
