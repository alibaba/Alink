package com.alibaba.alink.operator.stream.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.fm.FmModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.recommendation.FmPredictParams;

/**
 * Fm classifier predict stream operator. this operator predict data's label with fm model.
 */
public final class FmClassifierPredictStreamOp extends ModelMapStreamOp <FmClassifierPredictStreamOp>
	implements FmPredictParams <FmClassifierPredictStreamOp> {

	private static final long serialVersionUID = 392898558835257506L;

	public FmClassifierPredictStreamOp(BatchOperator model) {
		this(model, new Params());
	}

	public FmClassifierPredictStreamOp(BatchOperator model, Params params) {
		super(model, FmModelMapper::new, params);
	}

}
