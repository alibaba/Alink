package com.alibaba.alink.operator.batch.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.tree.BaseXGBoostTrainBatchOp;
import com.alibaba.alink.params.xgboost.XGBoostRegTrainParams;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;

@NameCn("XGBoost 回归训练")
@NameEn("XGBoost Regression Train")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.regression.XGBoostRegressor")
public final class XGBoostRegTrainBatchOp extends BaseXGBoostTrainBatchOp <XGBoostRegTrainBatchOp>
	implements XGBoostRegTrainParams <XGBoostRegTrainBatchOp> {

	public XGBoostRegTrainBatchOp() {
	}

	public XGBoostRegTrainBatchOp(Params params) {
		super(params);
	}
}
