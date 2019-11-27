package com.alibaba.alink.pipeline.regression;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.regression.GlmTrainBatchOp;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.params.regression.GlmPredictParams;
import com.alibaba.alink.params.regression.GlmTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * Generalized Linear Model.
 */
public class GeneralizedLinearRegression
	extends Trainer <GeneralizedLinearRegression, GeneralizedLinearRegressionModel>
	implements GlmTrainParams <GeneralizedLinearRegression>,
	GlmPredictParams <GeneralizedLinearRegression> {

	public GeneralizedLinearRegression() {
		super(new Params());
	}

	public GeneralizedLinearRegression(Params params) {
		super(params);
	}

	@Override
	protected BatchOperator train(BatchOperator in) {
		return new GlmTrainBatchOp(this.getParams()).linkFrom(in);
	}

}
