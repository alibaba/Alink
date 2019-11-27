package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.tree.predictors.GbdtModelMapper;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.params.classification.GbdtPredictParams;

/**
 * The batch operator that predict the data using the binary gbdt model.
 */
public final class GbdtPredictBatchOp extends ModelMapBatchOp <GbdtPredictBatchOp>
	implements GbdtPredictParams <GbdtPredictBatchOp> {
	public GbdtPredictBatchOp() {
		this(null);
	}

	public GbdtPredictBatchOp(Params params) {
		super(GbdtModelMapper::new, params);
	}
}
