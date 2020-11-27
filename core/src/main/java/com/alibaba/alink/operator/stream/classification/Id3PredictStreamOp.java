package com.alibaba.alink.operator.stream.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.tree.predictors.RandomForestModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.classification.Id3PredictParams;

/**
 * The stream operator that predict the data using the id3 model.
 */
public final class Id3PredictStreamOp extends ModelMapStreamOp <Id3PredictStreamOp>
	implements Id3PredictParams <Id3PredictStreamOp> {
	private static final long serialVersionUID = -9012245346513973803L;

	public Id3PredictStreamOp(BatchOperator model) {
		this(model, null);
	}

	public Id3PredictStreamOp(BatchOperator model, Params params) {
		super(model, RandomForestModelMapper::new, params);
	}
}
