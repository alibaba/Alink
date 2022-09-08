package com.alibaba.alink.operator.batch.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.lazy.WithModelInfoBatchOp;
import com.alibaba.alink.operator.common.linear.BaseLinearModelTrainBatchOp;
import com.alibaba.alink.operator.common.linear.LinearModelType;
import com.alibaba.alink.operator.common.linear.LinearRegressorModelInfo;
import com.alibaba.alink.params.regression.LassoRegTrainParams;

/**
 * Train a regression model with L1-regularization.
 */
@NameCn("Lasso回归训练")
@NameEn("Lasso Regression Training")
public final class LassoRegTrainBatchOp extends BaseLinearModelTrainBatchOp <LassoRegTrainBatchOp>
	implements LassoRegTrainParams <LassoRegTrainBatchOp>,
	WithModelInfoBatchOp <LinearRegressorModelInfo, LassoRegTrainBatchOp, LassoRegModelInfoBatchOp> {

	private static final long serialVersionUID = 2399798703219983298L;

	public LassoRegTrainBatchOp() {
		this(new Params());
	}

	public LassoRegTrainBatchOp(Params params) {
		super(params.clone(), LinearModelType.LinearReg, "LASSO");
	}

	@Override
	public LassoRegModelInfoBatchOp getModelInfoBatchOp() {
		return new LassoRegModelInfoBatchOp(this.getParams()).linkFrom(this);
	}
}
