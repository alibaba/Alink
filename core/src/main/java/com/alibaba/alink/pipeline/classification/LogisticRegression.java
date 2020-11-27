package com.alibaba.alink.pipeline.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.lazy.HasLazyPrintModelInfo;
import com.alibaba.alink.common.lazy.HasLazyPrintTrainInfo;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.LogisticRegressionTrainBatchOp;
import com.alibaba.alink.params.classification.LinearBinaryClassTrainParams;
import com.alibaba.alink.params.classification.LogisticRegressionPredictParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * Logistic regression is a popular method to predict a categorical response.
 */
public class LogisticRegression extends Trainer <LogisticRegression, LogisticRegressionModel> implements
	LinearBinaryClassTrainParams <LogisticRegression>,
	LogisticRegressionPredictParams <LogisticRegression>, HasLazyPrintTrainInfo <LogisticRegression>,
	HasLazyPrintModelInfo <LogisticRegression> {

	private static final long serialVersionUID = 5549946053432265218L;

	public LogisticRegression() {super();}

	public LogisticRegression(Params params) {
		super(params);
	}

	@Override
	protected BatchOperator <?> train(BatchOperator <?> in) {
		return new LogisticRegressionTrainBatchOp(getParams()).linkFrom(in);
	}
}
