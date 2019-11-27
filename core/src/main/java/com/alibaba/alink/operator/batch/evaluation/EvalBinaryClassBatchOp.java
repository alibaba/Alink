package com.alibaba.alink.operator.batch.evaluation;

import com.alibaba.alink.operator.common.evaluation.BaseEvalClassBatchOp;
import com.alibaba.alink.operator.common.evaluation.BinaryClassMetrics;
import com.alibaba.alink.operator.common.evaluation.EvaluationMetricsCollector;
import com.google.common.base.Preconditions;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.params.evaluation.BinaryEvaluationParams;

/**
 * Calculate the evaluation metrics for binary classifiction.
 *
 * You can either give label column and predResult column or give label column and predDetail column.
 * Once predDetail column is given, the predResult column is ignored.
 *
 * PositiveValue is optional, if given, it will be placed at the first position in the output label Array.
 * If not given, the labels are sorted in descending order.
 */
public class EvalBinaryClassBatchOp extends BaseEvalClassBatchOp<EvalBinaryClassBatchOp> implements
	BinaryEvaluationParams <EvalBinaryClassBatchOp>, EvaluationMetricsCollector<BinaryClassMetrics> {

	public EvalBinaryClassBatchOp() {
		this(null);
	}

	public EvalBinaryClassBatchOp(Params params) {
		super(params, true);
	}

	@Override
	public BinaryClassMetrics collectMetrics() {
		Preconditions.checkArgument(null != this.getOutputTable(), "Please provide the dataset to evaluate!");
		return new BinaryClassMetrics(this.collect().get(0));
	}
}
