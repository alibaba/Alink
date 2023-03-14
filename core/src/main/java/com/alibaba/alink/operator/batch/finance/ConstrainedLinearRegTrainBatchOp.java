package com.alibaba.alink.operator.batch.finance;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.operator.common.linear.BaseConstrainedLinearModelTrainBatchOp;
import com.alibaba.alink.operator.common.linear.LinearModelType;
import com.alibaba.alink.params.finance.ConstrainedLinearRegTrainParams;

/**
 * Train a regression model.
 */
@NameCn("带约束的线性回归训练")
@NameEn("Constrained Linear Selector Trainer")
public final class ConstrainedLinearRegTrainBatchOp
	extends BaseConstrainedLinearModelTrainBatchOp <ConstrainedLinearRegTrainBatchOp>
	implements ConstrainedLinearRegTrainParams <ConstrainedLinearRegTrainBatchOp> {

	private static final long serialVersionUID = 7603485759107349632L;

	public ConstrainedLinearRegTrainBatchOp() {
		this(new Params());
	}

	public ConstrainedLinearRegTrainBatchOp(Params params) {
		super(params, LinearModelType.LinearReg, "Linear Regression");
	}

}
