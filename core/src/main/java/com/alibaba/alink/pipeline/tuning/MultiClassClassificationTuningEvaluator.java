package com.alibaba.alink.pipeline.tuning;

import org.apache.flink.ml.api.misc.param.ParamInfo;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.evaluation.EvalMultiClassBatchOp;
import com.alibaba.alink.operator.common.evaluation.TuningMultiClassMetric;
import com.alibaba.alink.params.evaluation.EvalMultiClassParams;
import com.alibaba.alink.params.evaluation.HasTuningMultiClassMetric;

public class MultiClassClassificationTuningEvaluator extends TuningEvaluator<MultiClassClassificationTuningEvaluator>
	implements EvalMultiClassParams<MultiClassClassificationTuningEvaluator>,
	HasTuningMultiClassMetric<MultiClassClassificationTuningEvaluator> {

	public MultiClassClassificationTuningEvaluator() {
		super(null);
	}

	@Override
	public double evaluate(BatchOperator input) {
		return new EvalMultiClassBatchOp(getParams())
			.linkFrom(input)
			.collectMetrics()
			.getParams()
			.get(getMetricParamInfo());
	}

	@Override
	public boolean isLargerBetter() {
		return !(getTuningMultiClassMetric().equals(TuningMultiClassMetric.LOG_LOSS));
	}

	@Override
	ParamInfo<Double> getMetricParamInfo() {
		return getTuningMultiClassMetric().getMetricKey();
	}
}
