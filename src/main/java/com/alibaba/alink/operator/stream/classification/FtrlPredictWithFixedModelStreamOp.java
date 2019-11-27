package com.alibaba.alink.operator.stream.classification;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.linear.LinearModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.classification.LinearModelMapperParams;

import org.apache.flink.ml.api.misc.param.Params;

/**
 * Ftrl stream predictor with a model produced by batch ftrl algorithm.
 *
 */
public final class FtrlPredictWithFixedModelStreamOp extends ModelMapStreamOp <FtrlPredictWithFixedModelStreamOp>
	implements LinearModelMapperParams <FtrlPredictWithFixedModelStreamOp> {

	public FtrlPredictWithFixedModelStreamOp(BatchOperator model) {
		this(model, new Params());
	}

	public FtrlPredictWithFixedModelStreamOp(BatchOperator model, Params params) {
		super(model, LinearModelMapper::new, params);
	}

}
