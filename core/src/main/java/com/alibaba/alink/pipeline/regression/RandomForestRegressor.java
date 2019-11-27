package com.alibaba.alink.pipeline.regression;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.regression.RandomForestRegTrainBatchOp;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.params.regression.RandomForestRegPredictParams;
import com.alibaba.alink.params.regression.RandomForestRegTrainParams;
import com.alibaba.alink.pipeline.Trainer;

public class RandomForestRegressor extends Trainer <RandomForestRegressor, RandomForestRegressionModel>
	implements RandomForestRegTrainParams <RandomForestRegressor>,
	RandomForestRegPredictParams <RandomForestRegressor> {

	public RandomForestRegressor() {
		super();
	}

	public RandomForestRegressor(Params params) {
		super(params);
	}

	@Override
	protected BatchOperator train(BatchOperator in) {
		return new RandomForestRegTrainBatchOp(this.getParams()).linkFrom(in);
	}

}
