package com.alibaba.alink.operator.stream.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.tree.predictors.RandomForestModelMapper;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.regression.RegressorPredictParams;

public final class RandomForestsRegPredictStreamOp extends ModelMapStreamOp <RandomForestsRegPredictStreamOp>
	implements RegressorPredictParams <RandomForestsRegPredictStreamOp> {
	public RandomForestsRegPredictStreamOp(BatchOperator model) {
		this(model, null);
	}

	public RandomForestsRegPredictStreamOp(BatchOperator model, Params params) {
		super(model, RandomForestModelMapper::new, params);
	}
}
