package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.lazy.WithModelInfoBatchOp;
import com.alibaba.alink.operator.common.tree.BaseRandomForestTrainBatchOp;
import com.alibaba.alink.operator.common.tree.TreeModelInfo;
import com.alibaba.alink.operator.common.tree.TreeModelInfoBatchOp;
import com.alibaba.alink.operator.common.tree.TreeUtil;
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
	implements DecisionTreeTrainParams<DecisionTreeTrainBatchOp>,
	WithModelInfoBatchOp<TreeModelInfo.DecisionTreeModelInfo, DecisionTreeTrainBatchOp, TreeModelInfoBatchOp.DecisionTreeModelInfoBatchOp> {

	public DecisionTreeTrainBatchOp() {
		this(null);
	}

	public DecisionTreeTrainBatchOp(Params params) {
		super(params);
		getParams().set(
			TreeUtil.TREE_TYPE,
			TreeUtil.TreeType.valueOf(getParams().get(DecisionTreeTrainParams.TREE_TYPE).name())
		);
		getParams().set(RandomForestTrainParams.NUM_TREES, 1);
		getParams().set(HasFeatureSubsamplingRatio.FEATURE_SUBSAMPLING_RATIO, 1.0);
		getParams().set(HasSubsamplingRatio.SUBSAMPLING_RATIO, 1.0);
	}

	@Override
	public TreeModelInfoBatchOp.DecisionTreeModelInfoBatchOp getModelInfoBatchOp() {
		return new TreeModelInfoBatchOp.DecisionTreeModelInfoBatchOp(getParams()).linkFrom(this);
	}
}
