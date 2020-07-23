package com.alibaba.alink.operator.batch.evaluation;

import com.alibaba.alink.operator.common.evaluation.BaseEvalClassBatchOp;
import com.alibaba.alink.operator.common.evaluation.EvaluationMetricsCollector;
import com.alibaba.alink.operator.common.evaluation.MultiClassMetrics;
import com.alibaba.alink.params.evaluation.EvalMultiClassParams;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * Calculate the evaluation data for multi classifiction.
 *
 * You can either give label column and predResult column or give label column and predDetail column.
 * Once predDetail column is given, the predResult column is ignored.
 *
 * The labels are sorted in descending order in the output label array and confusion matrix..
 */
public class EvalMultiClassBatchOp extends BaseEvalClassBatchOp<EvalMultiClassBatchOp> implements
	EvalMultiClassParams<EvalMultiClassBatchOp>, EvaluationMetricsCollector<MultiClassMetrics, EvalMultiClassBatchOp> {

	public EvalMultiClassBatchOp() {
		this(null);
	}

	public EvalMultiClassBatchOp(Params params) {
		super(params, false);
	}

	@Override
	public MultiClassMetrics createMetrics(List<Row> rows){
		return new MultiClassMetrics(rows.get(0));
	}
}
