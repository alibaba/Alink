package com.alibaba.alink.pipeline.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.tree.predictors.XGBoostModelMapper;
import com.alibaba.alink.params.xgboost.XGBoostPredictParams;
import com.alibaba.alink.pipeline.MapModel;

@NameCn("XGBoost二分类训练模型")
public class XGBoostClassificationModel extends MapModel <XGBoostClassificationModel>
	implements XGBoostPredictParams <XGBoostClassificationModel> {

	private static final long serialVersionUID = -4935113216223290008L;

	public XGBoostClassificationModel() {this(null);}

	public XGBoostClassificationModel(Params params) {
		super(XGBoostModelMapper::new, params);
	}

}