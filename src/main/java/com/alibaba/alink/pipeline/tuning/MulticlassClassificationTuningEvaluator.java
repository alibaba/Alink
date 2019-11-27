package com.alibaba.alink.pipeline.tuning;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.evaluation.EvalMultiClassBatchOp;
import com.alibaba.alink.operator.common.evaluation.BaseSimpleClassifierMetrics;
import com.alibaba.alink.params.evaluation.MultiEvaluationParams;

public class MulticlassClassificationTuningEvaluator extends TuningEvaluator <MulticlassClassificationTuningEvaluator>
	implements MultiEvaluationParams <MulticlassClassificationTuningEvaluator> {

	public MulticlassClassificationTuningEvaluator() {
		super(null);
	}

	@Override
	public double evaluate(BatchOperator input) {
		return (double) new EvalMultiClassBatchOp(getParams())
			.linkFrom(input)
			.collectMetrics()
			.getParams()
			.get(findParamInfo(BaseSimpleClassifierMetrics.class, getMetricName()));
	}

	@Override
	public boolean isLargerBetter() {
		return true;
	}
}
