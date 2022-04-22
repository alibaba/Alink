package com.alibaba.alink.operator.batch.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.tree.predictors.XGBoostModelMapper;
import com.alibaba.alink.params.xgboost.XGBoostRegPredictParams;

@NameCn("XGBoost 回归预测")
@NameEn("XGBoost Regression Predict")
public final class XGBoostRegPredictBatchOp extends ModelMapBatchOp <XGBoostRegPredictBatchOp> implements
	XGBoostRegPredictParams <XGBoostRegPredictBatchOp> {

	public XGBoostRegPredictBatchOp() {
		this(new Params());
	}

	public XGBoostRegPredictBatchOp(Params params) {
		super(XGBoostModelMapper::new, params);
	}
}
