package com.alibaba.alink.operator.local.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.linear.LinearClassifierModelInfo;
import com.alibaba.alink.operator.common.linear.LinearModelType;
import com.alibaba.alink.operator.local.lazy.WithModelInfoLocalOp;
import com.alibaba.alink.params.classification.LinearBinaryClassTrainParams;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;

/**
 * Logistic regression train local operator. we use log loss func by setting LinearModelType = LR and model name =
 * "Logistic Regression".
 */
@NameCn("逻辑回归训练")
@NameEn("Logistic Regression Training")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.classification.LogisticRegression")
public final class LogisticRegressionTrainLocalOp extends BaseLinearModelTrainLocalOp <LogisticRegressionTrainLocalOp>
	implements LinearBinaryClassTrainParams <LogisticRegressionTrainLocalOp>,
	WithModelInfoLocalOp <LinearClassifierModelInfo, LogisticRegressionTrainLocalOp, LogisticRegressionModelInfoLocalOp> {

	public LogisticRegressionTrainLocalOp() {
		this(new Params());
	}

	public LogisticRegressionTrainLocalOp(Params params) {
		super(params, LinearModelType.LR, "Logistic Regression");
	}

	@Override
	public LogisticRegressionModelInfoLocalOp getModelInfoLocalOp() {
		return new LogisticRegressionModelInfoLocalOp(this.getParams()).linkFrom(this);
	}
}
