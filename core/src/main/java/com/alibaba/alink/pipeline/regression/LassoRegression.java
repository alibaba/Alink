package com.alibaba.alink.pipeline.regression;

import com.alibaba.alink.common.lazy.HasLazyPrintModelInfo;
import com.alibaba.alink.common.lazy.HasLazyPrintTrainInfo;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.regression.LassoRegTrainBatchOp;
import com.alibaba.alink.params.regression.LassoRegPredictParams;
import com.alibaba.alink.params.regression.LassoRegTrainParams;
import com.alibaba.alink.pipeline.Trainer;

import org.apache.flink.ml.api.misc.param.Params;

/**
 * Lasso regression pipeline op.
 *
 */
public class LassoRegression extends Trainer <LassoRegression, LassoRegressionModel> implements
	LassoRegTrainParams <LassoRegression>,
	LassoRegPredictParams <LassoRegression>,
	HasLazyPrintTrainInfo<LassoRegression>, HasLazyPrintModelInfo<LassoRegression> {

	private static final long serialVersionUID = -7444365100929167804L;

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
