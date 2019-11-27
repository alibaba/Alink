package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.tree.predictors.RandomForestModelMapper;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.params.classification.RandomForestPredictParams;

/**
 * The batch operator that predict the data using the random forest model.
 */
public final class RandomForestPredictBatchOp extends ModelMapBatchOp <RandomForestPredictBatchOp> implements
	RandomForestPredictParams <RandomForestPredictBatchOp> {
	public RandomForestPredictBatchOp() {
		this(null);
	}

	public RandomForestPredictBatchOp(Params params) {
		super(RandomForestModelMapper::new, params);
	}

}
