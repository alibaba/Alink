package com.alibaba.alink.pipeline.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.lazy.HasLazyPrintModelInfo;
import com.alibaba.alink.common.lazy.HasLazyPrintTrainInfo;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.regression.AftSurvivalRegTrainBatchOp;
import com.alibaba.alink.params.regression.AftRegPredictParams;
import com.alibaba.alink.params.regression.AftRegTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * Accelerated Failure Time Survival Regression.
 * Based on the Weibull distribution of the survival time.
 * <p>
 * (https://en.wikipedia.org/wiki/Accelerated_failure_time_model)
 */
@NameCn("生存回归")
public class AftSurvivalRegression extends Trainer <AftSurvivalRegression, AftSurvivalRegressionModel> implements
	AftRegTrainParams <AftSurvivalRegression>,
	AftRegPredictParams <AftSurvivalRegression>,
	HasLazyPrintTrainInfo <AftSurvivalRegression>, HasLazyPrintModelInfo <AftSurvivalRegression> {

	private static final long serialVersionUID = 3693171215775584261L;

	public AftSurvivalRegression() {
		super(new Params());
	}

	public AftSurvivalRegression(Params params) {
		super(params);
	}

	@Override
	protected BatchOperator <?> train(BatchOperator <?> in) {
		return new AftSurvivalRegTrainBatchOp(this.getParams()).linkFrom(in);
	}

}
