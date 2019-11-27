package com.alibaba.alink.operator.batch.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.tree.BaseRandomForestTrainBatchOp;
import com.alibaba.alink.params.regression.RandomForestRegTrainParams;
import com.alibaba.alink.params.shared.tree.HasTreeType;

/**
 *
 */
public final class RandomForestRegTrainBatchOp extends BaseRandomForestTrainBatchOp<RandomForestRegTrainBatchOp>
	implements RandomForestRegTrainParams<RandomForestRegTrainBatchOp> {

	public RandomForestRegTrainBatchOp() {
		this(null);
	}

	public RandomForestRegTrainBatchOp(Params params) {
		super(params);
		this.getParams().set(HasTreeType.TREE_TYPE, "mse");
	}
}
