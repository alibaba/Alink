package com.alibaba.alink.operator.batch.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.linear.LinearModelMapper;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.params.regression.LassoRegPredictParams;

/**
 * Lasso regression predict batch operator.
 *
 */
public final class LassoRegPredictBatchOp extends ModelMapBatchOp <LassoRegPredictBatchOp>
	implements LassoRegPredictParams <LassoRegPredictBatchOp> {

	public LassoRegPredictBatchOp() {
		this(new Params());
	}

	public LassoRegPredictBatchOp(Params params) {
		super(LinearModelMapper::new, params);
	}
}
