package com.alibaba.alink.operator.batch.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.batch.utils.WithModelInfoBatchOp;
import com.alibaba.alink.operator.common.linear.BaseLinearModelTrainBatchOp;
import com.alibaba.alink.operator.common.linear.LinearModelType;
import com.alibaba.alink.operator.common.linear.LinearRegressorModelInfo;
import com.alibaba.alink.params.regression.RidgeRegTrainParams;
import com.alibaba.alink.pipeline.EstimatorTrainerAnnotation;

/**
 * Train a regression model with L2-regularization.
 */
@NameCn("岭回归训练")
@NameEn("Linear Regression Training")
@EstimatorTrainerAnnotation(estimatorName = "com.alibaba.alink.pipeline.regression.RidgeRegression")
public final class RidgeRegTrainBatchOp extends BaseLinearModelTrainBatchOp <RidgeRegTrainBatchOp>
	implements RidgeRegTrainParams <RidgeRegTrainBatchOp>,
	WithModelInfoBatchOp <LinearRegressorModelInfo, RidgeRegTrainBatchOp, RidgeRegModelInfoBatchOp> {

	private static final long serialVersionUID = -1939712619795581386L;

	public RidgeRegTrainBatchOp() {
		this(new Params());
	}

	public RidgeRegTrainBatchOp(Params params) {
		super(params.clone(), LinearModelType.LinearReg, "Ridge Regression");
	}

	@Override
	public RidgeRegModelInfoBatchOp getModelInfoBatchOp() {
		return new RidgeRegModelInfoBatchOp(this.getParams()).linkFrom(this);
	}

}
