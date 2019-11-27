package com.alibaba.alink.operator.stream.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.linear.LinearModelMapper;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.classification.LinearSvmPredictParams;

/**
 * Linear svm predict stream operator. this operator predict data's label with linear model.
 *
 */
public final class LinearSvmPredictStreamOp extends ModelMapStreamOp <LinearSvmPredictStreamOp>
	implements LinearSvmPredictParams <LinearSvmPredictStreamOp> {

	public LinearSvmPredictStreamOp(BatchOperator model) {
		this(model, new Params());
	}

	public LinearSvmPredictStreamOp(BatchOperator model, Params params) {
		super(model, LinearModelMapper::new, params);
	}
}
