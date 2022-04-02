package com.alibaba.alink.operator.stream.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.tree.predictors.XGBoostModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.xgboost.XGBoostPredictParams;

@ParamSelectColumnSpec(name = "vectorCol", allowedTypeCollections = TypeCollections.VECTOR_TYPES)
public final class XGBoostPredictStreamOp extends ModelMapStreamOp <XGBoostPredictStreamOp>
	implements XGBoostPredictParams <XGBoostPredictStreamOp> {

	public XGBoostPredictStreamOp(BatchOperator <?> model) {
		this(model, new Params());
	}

	public XGBoostPredictStreamOp(BatchOperator <?> model, Params params) {
		super(model, XGBoostModelMapper::new, params);
	}
}
