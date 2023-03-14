package com.alibaba.alink.operator.local.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.linear.LinearModelType;
import com.alibaba.alink.operator.common.linear.LinearRegressorModelInfo;
import com.alibaba.alink.operator.local.classification.BaseLinearModelTrainLocalOp;
import com.alibaba.alink.operator.local.lazy.WithModelInfoLocalOp;
import com.alibaba.alink.params.regression.LinearRegTrainParams;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;

/**
 * Train a regression model.
 */
@NameCn("线性回归训练")
@NameEn("Linear Regression Training")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.regression.LinearRegression")
public final class LinearRegTrainLocalOp extends BaseLinearModelTrainLocalOp <LinearRegTrainLocalOp>
	implements LinearRegTrainParams <LinearRegTrainLocalOp>,
	WithModelInfoLocalOp <LinearRegressorModelInfo, LinearRegTrainLocalOp, LinearRegModelInfoLocalOp> {

	public LinearRegTrainLocalOp() {
		this(new Params());
	}

	public LinearRegTrainLocalOp(Params params) {
		super(params.clone(), LinearModelType.LinearReg, "Linear Regression");
	}

	@Override
	public LinearRegModelInfoLocalOp getModelInfoLocalOp() {
		return new LinearRegModelInfoLocalOp(this.getParams()).linkFrom(this);
	}
}
