package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.tree.BaseGbdtTrainBatchOp;

/**
 * Fit a binary classfication model.
 *
 * @see BaseGbdtTrainBatchOp
 */
public final class GbdtTrainBatchOp extends BaseGbdtTrainBatchOp<GbdtTrainBatchOp> {

	public GbdtTrainBatchOp() {
		algoType = 1;
	}

	public GbdtTrainBatchOp(Params params) {
		super(params);
		algoType = 1;
	}

}
