package com.alibaba.alink.pipeline.tuning;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.evaluation.EvalRegressionBatchOp;
import com.alibaba.alink.operator.common.evaluation.BaseSimpleRegressionMetrics;
import com.alibaba.alink.params.evaluation.RegressionEvaluationParams;
import org.apache.flink.ml.api.misc.param.ParamInfo;

public class RegressionTuningEvaluator extends TuningEvaluator <RegressionTuningEvaluator>
	implements RegressionEvaluationParams <RegressionTuningEvaluator> {

	public RegressionTuningEvaluator() {
		super(null);
	}

	@Override
	public double evaluate(BatchOperator in) {
		return (double) new EvalRegressionBatchOp(getParams())
			.linkFrom(in)
			.collectMetrics()
			.getParams()
			.get(findParamInfo(BaseSimpleRegressionMetrics.class, getMetricName()));
	}

	@Override
	public boolean isLargerBetter() {
		ParamInfo paramInfo = findParamInfo(BaseSimpleRegressionMetrics.class, getMetricName());
		return !(paramInfo.equals(BaseSimpleRegressionMetrics.MSE)
			|| paramInfo.equals(BaseSimpleRegressionMetrics.RMSE)
			|| paramInfo.equals(BaseSimpleRegressionMetrics.MAE));
	}
}
