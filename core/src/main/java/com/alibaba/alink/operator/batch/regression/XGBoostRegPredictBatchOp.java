package com.alibaba.alink.operator.batch.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.tree.predictors.XGBoostModelMapper;
import com.alibaba.alink.params.xgboost.XGBoostRegPredictParams;

public final class XGBoostRegPredictBatchOp extends ModelMapBatchOp <XGBoostRegPredictBatchOp> implements
	XGBoostRegPredictParams <XGBoostRegPredictBatchOp> {

	public XGBoostRegPredictBatchOp() {
		this(new Params());
	}

	public XGBoostRegPredictBatchOp(Params params) {
		super(XGBoostModelMapper::new, params);
	}
}
