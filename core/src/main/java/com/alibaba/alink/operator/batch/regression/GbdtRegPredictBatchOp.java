package com.alibaba.alink.operator.batch.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.tree.predictors.GbdtModelMapper;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.params.regression.GbdtRegPredictParams;

/**
 *
 */
public final class GbdtRegPredictBatchOp extends ModelMapBatchOp <GbdtRegPredictBatchOp>
	implements GbdtRegPredictParams <GbdtRegPredictBatchOp> {
	public GbdtRegPredictBatchOp() {
		this(null);
	}

	public GbdtRegPredictBatchOp(Params params) {
		super(GbdtModelMapper::new, params);
	}
}
