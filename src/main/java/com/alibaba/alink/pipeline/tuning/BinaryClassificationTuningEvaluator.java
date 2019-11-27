package com.alibaba.alink.pipeline.tuning;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.evaluation.EvalBinaryClassBatchOp;
import com.alibaba.alink.operator.common.evaluation.BinaryClassMetrics;
import com.alibaba.alink.params.evaluation.BinaryEvaluationParams;

public class BinaryClassificationTuningEvaluator extends TuningEvaluator <BinaryClassificationTuningEvaluator>
	implements BinaryEvaluationParams <BinaryClassificationTuningEvaluator> {

	public BinaryClassificationTuningEvaluator() {
		super(null);
	}

	@Override
	public double evaluate(BatchOperator in) {
		return (double) new EvalBinaryClassBatchOp(getParams())
			.linkFrom(in)
			.collectMetrics()
			.getParams()
			.get(findParamInfo(BinaryClassMetrics.class, getMetricName()));
	}

	@Override
	public boolean isLargerBetter() {
		return true;
	}
}
