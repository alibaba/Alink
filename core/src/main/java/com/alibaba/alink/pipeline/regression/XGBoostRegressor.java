package com.alibaba.alink.pipeline.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.regression.XGBoostRegTrainBatchOp;
import com.alibaba.alink.params.xgboost.XGBoostRegPredictParams;
import com.alibaba.alink.params.xgboost.XGBoostRegTrainParams;
import com.alibaba.alink.pipeline.Trainer;

@NameCn("XGBoost回归")
public class XGBoostRegressor extends Trainer <XGBoostRegressor, XGBoostRegressionModel> implements
	XGBoostRegTrainParams <XGBoostRegressor>,
	XGBoostRegPredictParams <XGBoostRegressor> {

	private static final long serialVersionUID = 7228606857064008240L;

	public XGBoostRegressor() {
		this(new Params());
	}

	public XGBoostRegressor(Params params) {
		super(params);
	}

	@Override
	protected BatchOperator <?> train(BatchOperator <?> in) {
		return new XGBoostRegTrainBatchOp(this.getParams()).linkFrom(in);
	}
}
