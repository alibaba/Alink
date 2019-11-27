package com.alibaba.alink.pipeline.regression;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.regression.RidgeRegTrainBatchOp;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.params.regression.RidgeRegPredictParams;
import com.alibaba.alink.params.regression.RidgeRegTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * Ridge regression pipeline op.
 *
 */
public class RidgeRegression extends Trainer <RidgeRegression, RidgeRegressionModel> implements
	RidgeRegTrainParams <RidgeRegression>,
	RidgeRegPredictParams <RidgeRegression> {

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
