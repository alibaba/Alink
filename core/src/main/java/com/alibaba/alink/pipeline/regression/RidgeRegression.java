package com.alibaba.alink.pipeline.regression;

import com.alibaba.alink.common.lazy.HasLazyPrintModelInfo;
import com.alibaba.alink.common.lazy.HasLazyPrintTrainInfo;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.regression.RidgeRegTrainBatchOp;
import com.alibaba.alink.params.regression.RidgeRegPredictParams;
import com.alibaba.alink.params.regression.RidgeRegTrainParams;
import com.alibaba.alink.pipeline.Trainer;

import org.apache.flink.ml.api.misc.param.Params;

/**
 * Ridge regression pipeline op.
 *
 */
public class RidgeRegression extends Trainer <RidgeRegression, RidgeRegressionModel> implements
	RidgeRegTrainParams <RidgeRegression>,
	RidgeRegPredictParams <RidgeRegression>,
	HasLazyPrintTrainInfo<RidgeRegression>, HasLazyPrintModelInfo<RidgeRegression> {

	private static final long serialVersionUID = -8067455400000733035L;

	public RidgeRegression() {
		super(new Params());
	}

	public RidgeRegression(Params params) {
		super(params);
	}

	@Override
	protected BatchOperator train(BatchOperator in) {
		return new RidgeRegTrainBatchOp(this.getParams()).linkFrom(in);
	}

}
