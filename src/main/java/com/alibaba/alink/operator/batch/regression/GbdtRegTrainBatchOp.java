package com.alibaba.alink.operator.batch.regression;

import com.alibaba.alink.operator.common.tree.BaseGbdtTrainBatchOp;
import org.apache.flink.ml.api.misc.param.Params;

/**
 *
 */
public class GbdtRegTrainBatchOp extends BaseGbdtTrainBatchOp <GbdtRegTrainBatchOp> {

	public GbdtRegTrainBatchOp() {
		algoType = 0;
	}

	public GbdtRegTrainBatchOp(Params params) {
		super(params);

		algoType = 0;
	}

}
