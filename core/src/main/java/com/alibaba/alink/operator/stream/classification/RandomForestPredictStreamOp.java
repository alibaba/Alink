package com.alibaba.alink.operator.stream.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.tree.predictors.RandomForestModelMapper;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.classification.RandomForestPredictParams;

public final class RandomForestPredictStreamOp extends ModelMapStreamOp <RandomForestPredictStreamOp>
	implements RandomForestPredictParams <RandomForestPredictStreamOp> {
	public RandomForestPredictStreamOp(BatchOperator model) {
		this(model, null);
	}

	public RandomForestPredictStreamOp(BatchOperator model, Params params) {
		super(model, RandomForestModelMapper::new, params);
	}
}
