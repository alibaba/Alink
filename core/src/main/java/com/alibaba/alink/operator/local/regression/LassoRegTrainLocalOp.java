package com.alibaba.alink.operator.local.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.linear.LinearModelType;
import com.alibaba.alink.operator.common.linear.LinearRegressorModelInfo;
import com.alibaba.alink.operator.local.classification.BaseLinearModelTrainLocalOp;
import com.alibaba.alink.operator.local.lazy.WithModelInfoLocalOp;
import com.alibaba.alink.params.regression.LassoRegTrainParams;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;

/**
 * Train a regression model with L1-regularization.
 */
@NameCn("Lasso回归训练")
@NameEn("Lasso Regression Training")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.regression.LassoRegression")
public final class LassoRegTrainLocalOp extends BaseLinearModelTrainLocalOp <LassoRegTrainLocalOp>
	implements LassoRegTrainParams <LassoRegTrainLocalOp>,
	WithModelInfoLocalOp <LinearRegressorModelInfo, LassoRegTrainLocalOp, LassoRegModelInfoLocalOp> {

	public LassoRegTrainLocalOp() {
		this(new Params());
	}

	public LassoRegTrainLocalOp(Params params) {
		super(params.clone(), LinearModelType.LinearReg, "LASSO");
	}

	@Override
	public LassoRegModelInfoLocalOp getModelInfoLocalOp() {
		return new LassoRegModelInfoLocalOp(this.getParams()).linkFrom(this);
	}
}
