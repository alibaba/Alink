package com.alibaba.alink.operator.stream.evaluation;

import com.alibaba.alink.operator.common.evaluation.BaseEvalClassStreamOp;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.params.evaluation.MultiEvaluationStreamParams;

/**
 * Calculate the evaluation data within time windows for multi classifiction.
 * You can either give label column and predResult column or give label column and predDetail column.
 * Once predDetail column is given, the predResult column is ignored.
 * The labels are sorted in descending order in the output label array and confusion matrix..
 */
public class EvalMultiClassStreamOp extends BaseEvalClassStreamOp<EvalMultiClassStreamOp> implements
	MultiEvaluationStreamParams <EvalMultiClassStreamOp> {

	public EvalMultiClassStreamOp() {
		this(null);
	}

	public EvalMultiClassStreamOp(Params params) {
		super(params, false);
	}
}
