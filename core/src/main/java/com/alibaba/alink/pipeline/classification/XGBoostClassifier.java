package com.alibaba.alink.pipeline.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.XGBoostTrainBatchOp;
import com.alibaba.alink.params.xgboost.XGBoostPredictParams;
import com.alibaba.alink.params.xgboost.XGBoostTrainParams;
import com.alibaba.alink.pipeline.Trainer;

public class XGBoostClassifier extends Trainer <XGBoostClassifier, XGBoostClassificationModel> implements
	XGBoostTrainParams <XGBoostClassifier>,
	XGBoostPredictParams <XGBoostClassifier> {

	private static final long serialVersionUID = 7228606857064008240L;

	public XGBoostClassifier() {
		this(new Params());
	}

	public XGBoostClassifier(Params params) {
		super(params);
	}

	@Override
	protected BatchOperator <?> train(BatchOperator <?> in) {
		return new XGBoostTrainBatchOp(this.getParams()).linkFrom(in);
	}
}
