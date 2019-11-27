package com.alibaba.alink.operator.stream.recommendation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.recommendation.AlsModelMapper;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.recommendation.AlsPredictParams;

/**
 * StreamOperator for ALS prediction.
 */
public final class AlsPredictStreamOp extends ModelMapStreamOp <AlsPredictStreamOp>
	implements AlsPredictParams <AlsPredictStreamOp> {

	public AlsPredictStreamOp(BatchOperator model) {
		this(model, new Params());
	}

	public AlsPredictStreamOp(BatchOperator model, Params params) {
		super(model, AlsModelMapper::new, params);
	}
}
