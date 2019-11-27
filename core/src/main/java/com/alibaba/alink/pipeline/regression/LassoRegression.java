package com.alibaba.alink.pipeline.regression;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.regression.LassoRegTrainBatchOp;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.params.regression.LassoRegPredictParams;
import com.alibaba.alink.params.regression.LassoRegTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * Lasso regression pipeline op.
 *
 */
public class LassoRegression extends Trainer <LassoRegression, LassoRegressionModel> implements
	LassoRegTrainParams <LassoRegression>,
	LassoRegPredictParams <LassoRegression> {

	public LassoRegression() {
		super(new Params());
	}

	public LassoRegression(Params params) {
		super(params);
	}

	@Override
	protected BatchOperator train(BatchOperator in) {
		return new LassoRegTrainBatchOp(this.getParams()).linkFrom(in);
	}

}
