package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.lazy.WithModelInfoBatchOp;
import com.alibaba.alink.operator.common.tree.BaseRandomForestTrainBatchOp;
import com.alibaba.alink.operator.common.tree.TreeModelInfo;
import com.alibaba.alink.operator.common.tree.TreeUtil;
import com.alibaba.alink.params.classification.Id3TrainParams;
import com.alibaba.alink.params.shared.tree.HasFeatureSubsamplingRatio;
import com.alibaba.alink.params.shared.tree.HasNumTreesDefaltAs10;
import com.alibaba.alink.params.shared.tree.HasSubsamplingRatio;

/**
 * Fit a id3 model.
 */
public final class Id3TrainBatchOp extends BaseRandomForestTrainBatchOp <Id3TrainBatchOp>
	implements Id3TrainParams <Id3TrainBatchOp>,
	WithModelInfoBatchOp <TreeModelInfo.DecisionTreeModelInfo, Id3TrainBatchOp, Id3ModelInfoBatchOp> {

	private static final long serialVersionUID = -6836925312967261330L;

	public Id3TrainBatchOp() {
		this(null);
	}

	public Id3TrainBatchOp(Params parameter) {
		super(parameter);

		getParams().set(TreeUtil.TREE_TYPE, TreeUtil.TreeType.INFOGAIN);
		getParams().set(HasNumTreesDefaltAs10.NUM_TREES, 1);
		getParams().set(HasFeatureSubsamplingRatio.FEATURE_SUBSAMPLING_RATIO, 1.0);
		getParams().set(HasSubsamplingRatio.SUBSAMPLING_RATIO, 1.0);
	}

	@Override
	public Id3ModelInfoBatchOp getModelInfoBatchOp() {
		return new Id3ModelInfoBatchOp(getParams()).linkFrom(this);
	}
}
