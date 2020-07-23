package com.alibaba.alink.operator.batch.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.lazy.WithModelInfoBatchOp;
import com.alibaba.alink.operator.batch.classification.RandomForestTrainBatchOp;
import com.alibaba.alink.operator.common.tree.BaseRandomForestTrainBatchOp;
import com.alibaba.alink.operator.common.tree.TreeModelInfo;
import com.alibaba.alink.operator.common.tree.TreeModelInfoBatchOp;
import com.alibaba.alink.operator.common.tree.TreeUtil;
import com.alibaba.alink.params.regression.RandomForestRegTrainParams;
import com.alibaba.alink.params.shared.tree.HasTreeType;

/**
 *
 */
public final class RandomForestRegTrainBatchOp extends BaseRandomForestTrainBatchOp<RandomForestRegTrainBatchOp>
	implements RandomForestRegTrainParams<RandomForestRegTrainBatchOp>,
	WithModelInfoBatchOp<TreeModelInfo.RandomForestModelInfo, RandomForestTrainBatchOp, TreeModelInfoBatchOp.RandomForestModelInfoBatchOp> {

	public RandomForestRegTrainBatchOp() {
		this(null);
	}

	public RandomForestRegTrainBatchOp(Params params) {
		super(params);
		this.getParams().set(TreeUtil.TREE_TYPE, TreeUtil.TreeType.MSE);
	}

	@Override
	public TreeModelInfoBatchOp.RandomForestModelInfoBatchOp getModelInfoBatchOp() {
		return new TreeModelInfoBatchOp.RandomForestModelInfoBatchOp(getParams()).linkFrom(this);
	}
}
