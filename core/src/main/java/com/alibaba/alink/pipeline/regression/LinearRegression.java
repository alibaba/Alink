package com.alibaba.alink.pipeline.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.lazy.HasLazyPrintModelInfo;
import com.alibaba.alink.common.lazy.HasLazyPrintTrainInfo;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.regression.LinearRegTrainBatchOp;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.operator.local.regression.LinearRegTrainLocalOp;
import com.alibaba.alink.params.regression.LinearRegPredictParams;
import com.alibaba.alink.params.regression.LinearRegTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * Linear regression pipeline op.
 */
@NameCn("线性回归")
public class LinearRegression extends Trainer <LinearRegression, LinearRegressionModel> implements
	LinearRegTrainParams <LinearRegression>,
	LinearRegPredictParams <LinearRegression>,
	HasLazyPrintTrainInfo <LinearRegression>, HasLazyPrintModelInfo <LinearRegression> {

	private static final long serialVersionUID = -6669772164060969665L;

	public LinearRegression() {
		super(new Params());
	}

	public LinearRegression(Params params) {
		super(params);
	}

	@Override
	protected BatchOperator <?> train(BatchOperator <?> in) {
		return new LinearRegTrainBatchOp(this.getParams()).linkFrom(in);
	}

	@Override
	protected LocalOperator <?> train(LocalOperator <?> in) {
		return new LinearRegTrainLocalOp(this.getParams()).linkFrom(in);
	}

}
