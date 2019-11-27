package com.alibaba.alink.operator.batch.classification;

import com.alibaba.alink.operator.common.linear.BaseLinearModelTrainBatchOp;
import com.alibaba.alink.operator.common.linear.LinearModelType;
import com.alibaba.alink.params.classification.LogisticRegressionTrainParams;

import org.apache.flink.ml.api.misc.param.Params;

/**
 * Logistic regression train batch operator. we use log loss func by setting LinearModelType = LR and model
 * name = "Logistic Regression".
 *
 */
public final class LogisticRegressionTrainBatchOp extends BaseLinearModelTrainBatchOp<LogisticRegressionTrainBatchOp>
	implements LogisticRegressionTrainParams <LogisticRegressionTrainBatchOp> {

	public LogisticRegressionTrainBatchOp() {
		this(new Params());
	}

	public LogisticRegressionTrainBatchOp(Params params) {
		super(params, LinearModelType.LR, "Logistic Regression");
	}
}
