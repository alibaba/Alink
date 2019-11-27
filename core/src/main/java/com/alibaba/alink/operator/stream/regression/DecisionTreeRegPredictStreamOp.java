package com.alibaba.alink.operator.stream.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.tree.predictors.RandomForestModelMapper;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.regression.DecisionTreeRegPredictParams;

public final class DecisionTreeRegPredictStreamOp extends ModelMapStreamOp <DecisionTreeRegPredictStreamOp>
	implements DecisionTreeRegPredictParams <DecisionTreeRegPredictStreamOp> {
	public DecisionTreeRegPredictStreamOp(BatchOperator model) {
		this(model, null);
	}

	public DecisionTreeRegPredictStreamOp(BatchOperator model, Params params) {
		super(model, RandomForestModelMapper::new, params);
	}
}
