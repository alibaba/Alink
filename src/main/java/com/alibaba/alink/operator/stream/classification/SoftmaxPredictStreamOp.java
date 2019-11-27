package com.alibaba.alink.operator.stream.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.linear.SoftmaxModelMapper;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.classification.SoftmaxPredictParams;

/**
 * Softmax predict stream operator. this operator predict data's label with linear model.
 *
 */
public final class SoftmaxPredictStreamOp extends ModelMapStreamOp <SoftmaxPredictStreamOp>
	implements SoftmaxPredictParams <SoftmaxPredictStreamOp> {

	public SoftmaxPredictStreamOp(BatchOperator model) {
		this(model, null);
	}

	public SoftmaxPredictStreamOp(BatchOperator model, Params params) {
		super(model, SoftmaxModelMapper::new, params);
	}

}
