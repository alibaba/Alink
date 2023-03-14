package com.alibaba.alink.operator.stream.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.feature.QuantileDiscretizerModelMapper;
import com.alibaba.alink.operator.common.fm.FmModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.recommendation.FmPredictParams;

/**
 * Fm classifier predict stream operator. this operator predict data's label with fm model.
 */
@ParamSelectColumnSpec(name = "vectorCol",
	allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@NameCn("FM分类预测")
@NameEn("FM Classification")
public final class FmClassifierPredictStreamOp extends ModelMapStreamOp <FmClassifierPredictStreamOp>
	implements FmPredictParams <FmClassifierPredictStreamOp> {

	private static final long serialVersionUID = 392898558835257506L;

	public FmClassifierPredictStreamOp() {
		super(FmModelMapper::new, new Params());
	}

	public FmClassifierPredictStreamOp(Params params) {
		super(FmModelMapper::new, params);
	}

	public FmClassifierPredictStreamOp(BatchOperator model) {
		this(model, new Params());
	}

	public FmClassifierPredictStreamOp(BatchOperator model, Params params) {
		super(model, FmModelMapper::new, params);
	}

}
