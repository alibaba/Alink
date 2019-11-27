package com.alibaba.alink.pipeline.regression;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.regression.LinearRegTrainBatchOp;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.params.regression.LinearRegPredictParams;
import com.alibaba.alink.params.regression.LinearRegTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * Linear regression pipeline op.
 *
 */
public class LinearRegression extends Trainer <LinearRegression, LinearRegressionModel> implements
	LinearRegTrainParams <LinearRegression>,
	LinearRegPredictParams <LinearRegression> {

	public LinearRegression() {
		super(new Params());
	}

	public LinearRegression(Params params) {
		super(params);
	}

	@Override
	protected BatchOperator train(BatchOperator in) {
		return new LinearRegTrainBatchOp(this.getParams()).linkFrom(in);
	}

}
