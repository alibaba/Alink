package com.alibaba.alink.pipeline.regression;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.regression.AftSurvivalRegTrainBatchOp;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.params.regression.AftRegPredictParams;
import com.alibaba.alink.params.regression.AftRegTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * Accelerated Failure Time Survival Regression.
 * Based on the Weibull distribution of the survival time.
 * <p>
 * (https://en.wikipedia.org/wiki/Accelerated_failure_time_model)
 */
public class AftSurvivalRegression extends Trainer <AftSurvivalRegression, AftSurvivalRegressionModel> implements
	AftRegTrainParams <AftSurvivalRegression>,
	AftRegPredictParams <AftSurvivalRegression> {

	public AftSurvivalRegression() {
		super(new Params());
	}

	public AftSurvivalRegression(Params params) {
		super(params);
	}

	@Override
	protected BatchOperator train(BatchOperator in) {
		return new AftSurvivalRegTrainBatchOp(this.getParams()).linkFrom(in);
	}

}
