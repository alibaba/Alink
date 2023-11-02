package com.alibaba.alink.operator.local.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.linear.LinearModelType;
import com.alibaba.alink.operator.common.linear.LinearRegressorModelInfo;
import com.alibaba.alink.operator.local.classification.BaseLinearModelTrainLocalOp;
import com.alibaba.alink.operator.local.lazy.WithModelInfoLocalOp;
import com.alibaba.alink.params.regression.RidgeRegTrainParams;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;

/**
 * Train a regression model with L2-regularization.
 */
@NameCn("岭回归训练")
@NameEn("Linear Regression Training")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.regression.RidgeRegression")
public final class RidgeRegTrainLocalOp extends BaseLinearModelTrainLocalOp <RidgeRegTrainLocalOp>
	implements RidgeRegTrainParams <RidgeRegTrainLocalOp>,
	WithModelInfoLocalOp <LinearRegressorModelInfo, RidgeRegTrainLocalOp, RidgeRegModelInfoLocalOp> {

	public RidgeRegTrainLocalOp() {
		this(new Params());
	}

	public RidgeRegTrainLocalOp(Params params) {
		super(params.clone(), LinearModelType.LinearReg, "Ridge Regression");
	}

	@Override
	public RidgeRegModelInfoLocalOp getModelInfoLocalOp() {
		return new RidgeRegModelInfoLocalOp(this.getParams()).linkFrom(this);
	}

}
