package com.alibaba.alink.operator.stream.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.fm.FmModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.recommendation.FmPredictParams;

/**
 * Fm regression predict stream operator. this operator predict data's label with fm model.
 */
@ParamSelectColumnSpec(name = "vectorCol",
	allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@NameCn("FM回归预测")
@NameEn("FM regression prediction")
public final class FmRegressorPredictStreamOp extends ModelMapStreamOp <FmRegressorPredictStreamOp>
	implements FmPredictParams <FmRegressorPredictStreamOp> {

	private static final long serialVersionUID = -3923538036043466470L;

	public FmRegressorPredictStreamOp() {
		super(FmModelMapper::new, new Params());
	}

	public FmRegressorPredictStreamOp(Params params) {
		super(FmModelMapper::new, params);
	}

	public FmRegressorPredictStreamOp(BatchOperator model) {
		this(model, new Params());
	}

	public FmRegressorPredictStreamOp(BatchOperator model, Params params) {
		super(model, FmModelMapper::new, params);
	}

}
