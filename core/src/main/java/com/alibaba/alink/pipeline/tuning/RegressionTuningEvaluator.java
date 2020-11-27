package com.alibaba.alink.pipeline.tuning;

import org.apache.flink.ml.api.misc.param.ParamInfo;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.evaluation.EvalRegressionBatchOp;
import com.alibaba.alink.operator.common.evaluation.TuningRegressionMetric;
import com.alibaba.alink.params.evaluation.EvalRegressionParams;
import com.alibaba.alink.params.evaluation.HasTuningRegressionMetric;

public class RegressionTuningEvaluator extends TuningEvaluator <RegressionTuningEvaluator>
	implements EvalRegressionParams <RegressionTuningEvaluator>,
	HasTuningRegressionMetric <RegressionTuningEvaluator> {

	public RegressionTuningEvaluator() {
		super(null);
	}

	@Override
	public double evaluate(BatchOperator <?> in) {
		return new EvalRegressionBatchOp(getParams())
			.linkFrom(in)
			.collectMetrics()
			.getParams()
			.get(getMetricParamInfo());
	}

	@Override
	public boolean isLargerBetter() {
		return !(getTuningRegressionMetric().equals(TuningRegressionMetric.MSE)
			|| getTuningRegressionMetric().equals(TuningRegressionMetric.RMSE)
			|| getTuningRegressionMetric().equals(TuningRegressionMetric.MAE)
			|| getTuningRegressionMetric().equals(TuningRegressionMetric.SAE)
			|| getTuningRegressionMetric().equals(TuningRegressionMetric.MAPE)
			|| getTuningRegressionMetric().equals(TuningRegressionMetric.EXPLAINED_VARIANCE)
		);
	}

	@Override
	ParamInfo <Double> getMetricParamInfo() {
		return getTuningRegressionMetric().getMetricKey();
	}
}
