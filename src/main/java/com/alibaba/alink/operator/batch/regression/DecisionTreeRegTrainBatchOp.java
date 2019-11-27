package com.alibaba.alink.operator.batch.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.tree.BaseRandomForestTrainBatchOp;
import com.alibaba.alink.params.regression.DecisionTreeRegTrainParams;
import com.alibaba.alink.params.shared.tree.HasFeatureSubsamplingRatio;
import com.alibaba.alink.params.shared.tree.HasNumTreesDefaltAs10;
import com.alibaba.alink.params.shared.tree.HasSubsamplingRatio;
import com.alibaba.alink.params.shared.tree.HasTreeType;

/**
 *
 */
public final class DecisionTreeRegTrainBatchOp extends BaseRandomForestTrainBatchOp<DecisionTreeRegTrainBatchOp>
	implements DecisionTreeRegTrainParams<DecisionTreeRegTrainBatchOp> {

	public DecisionTreeRegTrainBatchOp() {
		this(null);
	}

	public DecisionTreeRegTrainBatchOp(Params params) {
		super(params);
		getParams().set(HasNumTreesDefaltAs10.NUM_TREES, 1);
		getParams().set(HasTreeType.TREE_TYPE, "mse");
		getParams().set(HasFeatureSubsamplingRatio.FEATURE_SUBSAMPLING_RATIO, 1.0);
		getParams().set(HasSubsamplingRatio.SUBSAMPLING_RATIO, 1.0);
	}
}
