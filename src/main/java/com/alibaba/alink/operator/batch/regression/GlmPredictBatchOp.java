package com.alibaba.alink.operator.batch.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.regression.GlmModelMapper;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.params.regression.GlmPredictParams;

/**
 * Generalized Linear Model.
 */
public class GlmPredictBatchOp extends ModelMapBatchOp <GlmPredictBatchOp>
	implements GlmPredictParams <GlmPredictBatchOp> {
	public GlmPredictBatchOp() {
		this(new Params());
	}

	public GlmPredictBatchOp(Params params) {
		super(GlmModelMapper::new, params);
	}
}
