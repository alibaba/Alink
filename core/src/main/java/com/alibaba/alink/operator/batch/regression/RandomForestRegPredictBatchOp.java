package com.alibaba.alink.operator.batch.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.tree.predictors.RandomForestModelMapper;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.params.regression.RandomForestRegPredictParams;

/**
 *
 */
public final class RandomForestRegPredictBatchOp extends ModelMapBatchOp <RandomForestRegPredictBatchOp> implements
	RandomForestRegPredictParams <RandomForestRegPredictBatchOp> {
	public RandomForestRegPredictBatchOp() {
		this(null);
	}

	public RandomForestRegPredictBatchOp(Params params) {
		super(RandomForestModelMapper::new, params);
	}
}
