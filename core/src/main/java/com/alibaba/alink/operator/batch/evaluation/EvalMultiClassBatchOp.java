package com.alibaba.alink.operator.batch.evaluation;

import com.alibaba.alink.operator.common.evaluation.BaseEvalClassBatchOp;
import com.alibaba.alink.operator.common.evaluation.EvaluationMetricsCollector;
import com.alibaba.alink.operator.common.evaluation.MultiClassMetrics;
import com.google.common.base.Preconditions;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.params.evaluation.MultiEvaluationParams;

/**
 * Calculate the evaluation data for multi classifiction.
 *
 * You can either give label column and predResult column or give label column and predDetail column.
 * Once predDetail column is given, the predResult column is ignored.
 *
 * The labels are sorted in descending order in the output label array and confusion matrix..
 */
public class EvalMultiClassBatchOp extends BaseEvalClassBatchOp<EvalMultiClassBatchOp> implements
	MultiEvaluationParams <EvalMultiClassBatchOp>, EvaluationMetricsCollector<MultiClassMetrics> {

	public EvalMultiClassBatchOp() {
		this(null);
	}

	public EvalMultiClassBatchOp(Params params) {
		super(params, false);
	}

	@Override
	public MultiClassMetrics collectMetrics() {
		Preconditions.checkArgument(null != this.getOutputTable(), "Please provide the dataset to evaluate!");
		return new MultiClassMetrics(this.collect().get(0));
	}
}
