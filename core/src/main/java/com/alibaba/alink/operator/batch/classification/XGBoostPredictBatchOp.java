package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.tree.predictors.XGBoostModelMapper;
import com.alibaba.alink.params.xgboost.XGBoostPredictParams;

@ParamSelectColumnSpec(name = "vectorCol", allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@NameCn("XGBoost二分类预测")
@NameEn("XGBoost Binary Classification Prediction")
public final class XGBoostPredictBatchOp extends ModelMapBatchOp <XGBoostPredictBatchOp> implements
	XGBoostPredictParams <XGBoostPredictBatchOp> {

	public XGBoostPredictBatchOp() {
		this(new Params());
	}

	public XGBoostPredictBatchOp(Params params) {
		super(XGBoostModelMapper::new, params);
	}
}
