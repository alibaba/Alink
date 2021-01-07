package com.alibaba.alink.pipeline.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.lazy.HasLazyPrintModelInfo;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.regression.GlmTrainBatchOp;
import com.alibaba.alink.params.regression.GlmPredictParams;
import com.alibaba.alink.params.regression.GlmTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * Generalized Linear Model. https://en.wikipedia.org/wiki/Generalized_linear_model.
 */
public class GeneralizedLinearRegression
	extends Trainer <GeneralizedLinearRegression, GeneralizedLinearRegressionModel>
	implements GlmTrainParams <GeneralizedLinearRegression>,
	GlmPredictParams <GeneralizedLinearRegression>,
	HasLazyPrintModelInfo <GeneralizedLinearRegression> {

	private static final long serialVersionUID = 217074066645415654L;

	public GeneralizedLinearRegression() {
		super(new Params());
	}

	public GeneralizedLinearRegression(Params params) {
		super(params);
	}

	@Override
	protected BatchOperator <?> train(BatchOperator <?> in) {
		return new GlmTrainBatchOp(this.getParams()).linkFrom(in);
	}

}
