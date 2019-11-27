package com.alibaba.alink.pipeline.classification;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.LogisticRegressionTrainBatchOp;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.params.classification.LogisticRegressionPredictParams;
import com.alibaba.alink.params.classification.LogisticRegressionTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * Logistic regression is a popular method to predict a categorical response.
 *
 */
public class LogisticRegression extends Trainer <LogisticRegression, LogisticRegressionModel> implements
	LogisticRegressionTrainParams <LogisticRegression>,
	LogisticRegressionPredictParams <LogisticRegression> {

	public LogisticRegression() {super();}

	public LogisticRegression(Params params) {
		super(params);
	}

	@Override
	protected BatchOperator train(BatchOperator in) {
		return new LogisticRegressionTrainBatchOp(getParams()).linkFrom(in);
	}
}
