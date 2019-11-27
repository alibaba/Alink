package com.alibaba.alink.pipeline.regression;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.regression.DecisionTreeRegTrainBatchOp;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.params.regression.DecisionTreeRegPredictParams;
import com.alibaba.alink.params.regression.DecisionTreeRegTrainParams;
import com.alibaba.alink.pipeline.Trainer;

public class DecisionTreeRegressor extends Trainer <DecisionTreeRegressor, DecisionTreeRegressionModel> implements
	DecisionTreeRegTrainParams <DecisionTreeRegressor>, DecisionTreeRegPredictParams <DecisionTreeRegressor> {

	public DecisionTreeRegressor() {
		super();
	}

	public DecisionTreeRegressor(Params params) {
		super(params);
	}

	@Override
	protected BatchOperator train(BatchOperator in) {
		return new DecisionTreeRegTrainBatchOp(this.getParams()).linkFrom(in);
	}

}
