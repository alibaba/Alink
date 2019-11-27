package com.alibaba.alink.pipeline.regression;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.regression.GbdtRegTrainBatchOp;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.params.regression.GbdtRegPredictParams;
import com.alibaba.alink.params.regression.GbdtRegTrainParams;
import com.alibaba.alink.pipeline.Trainer;

public class GbdtRegressor extends Trainer <GbdtRegressor, GbdtRegressionModel> implements
	GbdtRegTrainParams <GbdtRegressor>,
	GbdtRegPredictParams <GbdtRegressor> {

	public GbdtRegressor() {
		this(new Params());
	}

	public GbdtRegressor(Params params) {
		super(params);
	}

	@Override
	protected BatchOperator train(BatchOperator in) {
		return new GbdtRegTrainBatchOp(this.getParams()).linkFrom(in);
	}
}
