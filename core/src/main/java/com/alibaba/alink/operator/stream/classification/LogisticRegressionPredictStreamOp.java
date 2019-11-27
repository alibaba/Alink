package com.alibaba.alink.operator.stream.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.linear.LinearModelMapper;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.classification.LogisticRegressionPredictParams;

/**
 * Linear logistic regression predict stream operator. this operator predict data's label with linear model.
 *
 */
public final class LogisticRegressionPredictStreamOp extends ModelMapStreamOp <LogisticRegressionPredictStreamOp>
	implements LogisticRegressionPredictParams <LogisticRegressionPredictStreamOp> {

	public LogisticRegressionPredictStreamOp(BatchOperator model) {
		this(model, new Params());
	}

	public LogisticRegressionPredictStreamOp(BatchOperator model, Params params) {
		super(model, LinearModelMapper::new, params);
	}
}



