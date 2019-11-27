package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.tree.predictors.RandomForestModelMapper;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.params.classification.DecisionTreePredictParams;

/**
 * The batch operator that predict the data using the single decision tree.
 */
public final class DecisionTreePredictBatchOp extends ModelMapBatchOp <DecisionTreePredictBatchOp> implements
	DecisionTreePredictParams <DecisionTreePredictBatchOp> {
	public DecisionTreePredictBatchOp() {
		this(null);
	}

	public DecisionTreePredictBatchOp(Params params) {
		super(RandomForestModelMapper::new, params);
	}
}
