package com.alibaba.alink.operator.stream.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.linear.LinearModelMapper;
import com.alibaba.alink.operator.common.recommendation.RecommendationRankingMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.regression.RidgeRegPredictParams;

/**
 * Ridge regression predict stream operator. this operator predict data's regression value with linear model.
 */
@ParamSelectColumnSpec(name = "vectorCol",
	allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@NameCn("岭回归预测")
public class RidgeRegPredictStreamOp extends ModelMapStreamOp <RidgeRegPredictStreamOp>
	implements RidgeRegPredictParams <RidgeRegPredictStreamOp> {

	private static final long serialVersionUID = -6544048132102142094L;

	public RidgeRegPredictStreamOp() {
		super(LinearModelMapper::new, new Params());
	}

	public RidgeRegPredictStreamOp(Params params) {
		super(LinearModelMapper::new, params);
	}

	public RidgeRegPredictStreamOp(BatchOperator model) {
		super(model, LinearModelMapper::new, new Params());
	}

	public RidgeRegPredictStreamOp(BatchOperator model, Params params) {
		super(model, LinearModelMapper::new, params);
	}

}
