package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.linear.LinearModelMapper;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.params.classification.LinearSvmPredictParams;

/**
 * Linear svm predict batch operator. this operator predict data's label with linear model.
 *
 */
public final class LinearSvmPredictBatchOp extends ModelMapBatchOp <LinearSvmPredictBatchOp>
	implements LinearSvmPredictParams <LinearSvmPredictBatchOp> {

	public LinearSvmPredictBatchOp() {
		this(new Params());
	}

	public LinearSvmPredictBatchOp(Params params) {
		super(LinearModelMapper::new, params);
	}

}
