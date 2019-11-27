package com.alibaba.alink.operator.stream.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.tree.predictors.RandomForestModelMapper;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.classification.DecisionTreePredictParams;

/**
 * The stream operator that predict the data using the single decision tree.
 */
public final class DecisionTreePredictStreamOp extends ModelMapStreamOp <DecisionTreePredictStreamOp> implements
	DecisionTreePredictParams <DecisionTreePredictStreamOp> {
	public DecisionTreePredictStreamOp(BatchOperator model) {
		this(model, null);
	}

	public DecisionTreePredictStreamOp(BatchOperator model, Params params) {
		super(model, RandomForestModelMapper::new, params);
	}
}
