package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.lazy.WithModelInfoBatchOp;
import com.alibaba.alink.operator.common.tree.BaseRandomForestTrainBatchOp;
import com.alibaba.alink.operator.common.tree.TreeModelInfo;
import com.alibaba.alink.params.classification.RandomForestTrainParams;

/**
 * Fit a random forest classification model.
 *
 * @see BaseRandomForestTrainBatchOp
 */
public final class RandomForestTrainBatchOp extends BaseRandomForestTrainBatchOp <RandomForestTrainBatchOp> implements
	RandomForestTrainParams <RandomForestTrainBatchOp>,
	WithModelInfoBatchOp <TreeModelInfo.RandomForestModelInfo, RandomForestTrainBatchOp, RandomForestModelInfoBatchOp> {

	private static final long serialVersionUID = 608820075935409265L;

	public RandomForestTrainBatchOp() {
		this(null);
	}

	public RandomForestTrainBatchOp(Params params) {
		super(params);
	}

	@Override
	public RandomForestModelInfoBatchOp getModelInfoBatchOp() {
		return new RandomForestModelInfoBatchOp(getParams()).linkFrom(this);
	}
}
