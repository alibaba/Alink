package com.alibaba.alink.operator.batch.finance;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.linear.BaseConstrainedLinearModelTrainBatchOp;
import com.alibaba.alink.operator.common.linear.LinearModelType;
import com.alibaba.alink.params.finance.ConstrainedLogisticRegressionTrainParams;

/**
 * Logistic regression train batch operator. we use log loss func by setting LinearModelType = LR and model
 * name = "Logistic Regression".
 */
@NameCn("带约束的逻辑回归训练")
@NameEn("Constrained Logistic Regression Trainer")
public final class ConstrainedLogisticRegressionTrainBatchOp
	extends BaseConstrainedLinearModelTrainBatchOp <ConstrainedLogisticRegressionTrainBatchOp>
	implements ConstrainedLogisticRegressionTrainParams <ConstrainedLogisticRegressionTrainBatchOp> {
	private static final long serialVersionUID = 3324942229315576654L;

	public ConstrainedLogisticRegressionTrainBatchOp() {
		this(new Params());
	}

	public ConstrainedLogisticRegressionTrainBatchOp(Params params) {
		super(params, LinearModelType.LR, "Logistic Regression");
	}
}
