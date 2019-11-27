package com.alibaba.alink.operator.batch.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.linear.LinearModelMapper;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.params.regression.LinearRegPredictParams;

/**
 * Linear regression predict batch operator.
 *
 */
public final class LinearRegPredictBatchOp extends ModelMapBatchOp <LinearRegPredictBatchOp>
	implements LinearRegPredictParams <LinearRegPredictBatchOp> {

	public LinearRegPredictBatchOp() {
		this(new Params());
	}

	public LinearRegPredictBatchOp(Params params) {
		super(LinearModelMapper::new, params);
	}
}
