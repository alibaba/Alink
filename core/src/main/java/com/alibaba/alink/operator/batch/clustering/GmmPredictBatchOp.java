package com.alibaba.alink.operator.batch.clustering;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.clustering.GmmModelMapper;
import com.alibaba.alink.params.clustering.GmmPredictParams;

/**
 * Gaussian Mixture prediction based on the model fitted by GmmTrainBatchOp.
 */
public final class GmmPredictBatchOp extends ModelMapBatchOp <GmmPredictBatchOp>
	implements GmmPredictParams <GmmPredictBatchOp> {

	private static final long serialVersionUID = 6478692106410428427L;

	public GmmPredictBatchOp() {
		this(null);
	}

	public GmmPredictBatchOp(Params params) {
		super(GmmModelMapper::new, params);
	}
}
