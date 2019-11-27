package com.alibaba.alink.operator.stream.evaluation;

import com.alibaba.alink.operator.common.evaluation.BaseEvalClassStreamOp;
import org.apache.flink.ml.api.misc.param.Params;
import com.alibaba.alink.params.evaluation.BinaryEvaluationStreamParams;

/**
 * Calculate the evaluation data within time windows for binary classifiction.
 * You can either give label column and predResult column or give label column and predDetail column.
 * Once predDetail column is given, the predResult column is ignored.
 * PositiveValue is optional, if given, it will placed at the first position in the output label Array.
 * If not given, the labels are sorted in descending order.
 */
public class EvalBinaryClassStreamOp extends BaseEvalClassStreamOp<EvalBinaryClassStreamOp> implements
	BinaryEvaluationStreamParams <EvalBinaryClassStreamOp> {

	public EvalBinaryClassStreamOp() {
		this(null);
	}

	public EvalBinaryClassStreamOp(Params params) {
		super(params, true);
	}
}
