package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.utils.WithModelInfoBatchOp;
import com.alibaba.alink.operator.common.linear.BaseLinearModelTrainBatchOp;
import com.alibaba.alink.operator.common.linear.LinearClassifierModelInfo;
import com.alibaba.alink.operator.common.linear.LinearModelType;
import com.alibaba.alink.params.classification.LinearBinaryClassTrainParams;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;

/**
 * Logistic regression train batch operator. we use log loss func by setting LinearModelType = LR and model
 * name = "Logistic Regression".
 */
@NameCn("逻辑回归训练")
@NameEn("Logistic Regression Training")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.classification.LogisticRegression")
public final class LogisticRegressionTrainBatchOp extends BaseLinearModelTrainBatchOp <LogisticRegressionTrainBatchOp>
	implements LinearBinaryClassTrainParams <LogisticRegressionTrainBatchOp>,
	WithModelInfoBatchOp <LinearClassifierModelInfo, LogisticRegressionTrainBatchOp,
		LogisticRegressionModelInfoBatchOp> {

	private static final long serialVersionUID = 2337925921060991692L;

	public LogisticRegressionTrainBatchOp() {
		this(new Params());
	}

	public LogisticRegressionTrainBatchOp(Params params) {
		super(params, LinearModelType.LR, "Logistic Regression");
	}

	@Override
	public LogisticRegressionModelInfoBatchOp getModelInfoBatchOp() {
		return new LogisticRegressionModelInfoBatchOp(this.getParams()).linkFrom(this);
	}
}
