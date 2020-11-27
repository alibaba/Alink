package com.alibaba.alink.operator.stream.clustering;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.clustering.GmmModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.clustering.GmmPredictParams;

/**
 * Gaussian Mixture prediction based on the model fitted by GmmTrainBatchOp.
 */
public final class GmmPredictStreamOp extends ModelMapStreamOp <GmmPredictStreamOp>
	implements GmmPredictParams <GmmPredictStreamOp> {

	private static final long serialVersionUID = -6737997176873713411L;

	public GmmPredictStreamOp(BatchOperator model) {
		this(model, new Params());
	}

	public GmmPredictStreamOp(BatchOperator model, Params params) {
		super(model, GmmModelMapper::new, params);
	}
}
