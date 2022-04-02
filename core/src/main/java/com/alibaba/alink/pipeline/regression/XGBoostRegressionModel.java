package com.alibaba.alink.pipeline.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.tree.predictors.XGBoostModelMapper;
import com.alibaba.alink.params.xgboost.XGBoostRegPredictParams;
import com.alibaba.alink.pipeline.MapModel;

public class XGBoostRegressionModel extends MapModel <XGBoostRegressionModel>
	implements XGBoostRegPredictParams <XGBoostRegressionModel> {

	private static final long serialVersionUID = -4935113216223290008L;

	public XGBoostRegressionModel() {this(null);}

	public XGBoostRegressionModel(Params params) {
		super(XGBoostModelMapper::new, params);
	}

}