package com.alibaba.alink.operator.batch.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.linear.LinearModelMapper;
import com.alibaba.alink.params.regression.RidgeRegPredictParams;

/**
 * Ridge regression predict batch operator.
 */
public final class RidgeRegPredictBatchOp extends ModelMapBatchOp <RidgeRegPredictBatchOp>
	implements RidgeRegPredictParams <RidgeRegPredictBatchOp> {

	private static final long serialVersionUID = 1294548322830314561L;

	public RidgeRegPredictBatchOp() {
		super(LinearModelMapper::new, new Params());
	}

	public RidgeRegPredictBatchOp(Params params) {
		super(LinearModelMapper::new, params);
	}
}
