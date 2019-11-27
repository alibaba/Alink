package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.tree.BaseRandomForestTrainBatchOp;
import com.alibaba.alink.params.classification.DecisionTreeTrainParams;
import com.alibaba.alink.params.classification.RandomForestTrainParams;
import com.alibaba.alink.params.shared.tree.HasFeatureSubsamplingRatio;
import com.alibaba.alink.params.shared.tree.HasSubsamplingRatio;

/**
 * Fit a single decision tree.
 *
 * @see BaseRandomForestTrainBatchOp
 */
public final class DecisionTreeTrainBatchOp extends BaseRandomForestTrainBatchOp<DecisionTreeTrainBatchOp>
	implements DecisionTreeTrainParams<DecisionTreeTrainBatchOp> {

	public DecisionTreeTrainBatchOp() {
		this(null);
	}

	public DecisionTreeTrainBatchOp(Params params) {
		super(params);
		getParams().set(RandomForestTrainParams.NUM_TREES, 1);
		getParams().set(HasFeatureSubsamplingRatio.FEATURE_SUBSAMPLING_RATIO, 1.0);
		getParams().set(HasSubsamplingRatio.SUBSAMPLING_RATIO, 1.0);
	}
}
