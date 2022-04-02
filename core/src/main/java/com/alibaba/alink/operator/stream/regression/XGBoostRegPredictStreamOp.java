package com.alibaba.alink.operator.stream.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.tree.predictors.XGBoostModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.xgboost.XGBoostRegPredictParams;

public final class XGBoostRegPredictStreamOp extends ModelMapStreamOp <XGBoostRegPredictStreamOp>
	implements XGBoostRegPredictParams <XGBoostRegPredictStreamOp> {

	public XGBoostRegPredictStreamOp(BatchOperator <?> model) {
		this(model, new Params());
	}

	public XGBoostRegPredictStreamOp(BatchOperator <?> model, Params params) {
		super(model, XGBoostModelMapper::new, params);
	}
}
