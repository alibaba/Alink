package com.alibaba.alink.operator.stream.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.tree.predictors.XGBoostModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.xgboost.XGBoostRegPredictParams;

@NameCn("XGBoost 回归预测")
@NameEn("XGBoost Regression Predict")
public final class XGBoostRegPredictStreamOp extends ModelMapStreamOp <XGBoostRegPredictStreamOp>
	implements XGBoostRegPredictParams <XGBoostRegPredictStreamOp> {

	public XGBoostRegPredictStreamOp() {
		super(XGBoostModelMapper::new, new Params());
	}

	public XGBoostRegPredictStreamOp(Params params) {
		super(XGBoostModelMapper::new, params);
	}

	public XGBoostRegPredictStreamOp(BatchOperator <?> model) {
		this(model, new Params());
	}

	public XGBoostRegPredictStreamOp(BatchOperator <?> model, Params params) {
		super(model, XGBoostModelMapper::new, params);
	}
}
