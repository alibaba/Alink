package com.alibaba.alink.pipeline.tuning;

import org.apache.flink.ml.api.misc.param.ParamInfo;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.evaluation.EvalBinaryClassBatchOp;
import com.alibaba.alink.operator.common.evaluation.TuningBinaryClassMetric;
import com.alibaba.alink.params.evaluation.EvalBinaryClassParams;
import com.alibaba.alink.params.evaluation.HasTuningBinaryClassMetric;

public class BinaryClassificationTuningEvaluator extends TuningEvaluator <BinaryClassificationTuningEvaluator>
	implements EvalBinaryClassParams <BinaryClassificationTuningEvaluator>,
	HasTuningBinaryClassMetric <BinaryClassificationTuningEvaluator> {

	public BinaryClassificationTuningEvaluator() {
		super(null);
	}

	@Override
	public double evaluate(BatchOperator <?> in) {
		return new EvalBinaryClassBatchOp(getParams())
			.linkFrom(in)
			.collectMetrics()
			.getParams()
			.get(getMetricParamInfo());
	}

	@Override
	public boolean isLargerBetter() {
		return !(getTuningBinaryClassMetric().equals(TuningBinaryClassMetric.LOG_LOSS));
	}

	@Override
	ParamInfo <Double> getMetricParamInfo() {
		return getTuningBinaryClassMetric().getMetricKey();
	}
}
