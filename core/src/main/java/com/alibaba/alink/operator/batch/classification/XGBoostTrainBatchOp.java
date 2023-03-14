package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.tree.BaseXGBoostTrainBatchOp;
import com.alibaba.alink.params.xgboost.XGBoostTrainParams;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;

@NameCn("XGBoost二分类训练")
@NameEn("XGBoost Binary Classification Training")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.classification.XGBoostClassifier")
public final class XGBoostTrainBatchOp extends BaseXGBoostTrainBatchOp <XGBoostTrainBatchOp>
	implements XGBoostTrainParams <XGBoostTrainBatchOp> {

	public XGBoostTrainBatchOp() {
		this(new Params());
	}

	public XGBoostTrainBatchOp(Params params) {
		super(params);

		if (!getParams().contains(XGBoostTrainParams.OBJECTIVE)) {
			getParams().set(XGBoostTrainParams.OBJECTIVE, Objective.BINARY_LOGISTIC);
		}
	}
}
