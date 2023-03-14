package com.alibaba.alink.operator.stream.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.nlp.Word2VecModelMapper;
import com.alibaba.alink.operator.common.tree.predictors.XGBoostModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.xgboost.XGBoostPredictParams;

@NameCn("XGBoost 分类预测")
@NameEn("XGBoost Prediction")
@ParamSelectColumnSpec(name = "vectorCol", allowedTypeCollections = TypeCollections.VECTOR_TYPES)
public final class XGBoostPredictStreamOp extends ModelMapStreamOp <XGBoostPredictStreamOp>
	implements XGBoostPredictParams <XGBoostPredictStreamOp> {

	public XGBoostPredictStreamOp() {
		super(XGBoostModelMapper::new, new Params());
	}

	public XGBoostPredictStreamOp(Params params) {
		super(XGBoostModelMapper::new, params);
	}

	public XGBoostPredictStreamOp(BatchOperator <?> model) {
		this(model, new Params());
	}

	public XGBoostPredictStreamOp(BatchOperator <?> model, Params params) {
		super(model, XGBoostModelMapper::new, params);
	}
}
