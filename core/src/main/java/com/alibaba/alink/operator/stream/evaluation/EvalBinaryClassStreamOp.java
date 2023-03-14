package com.alibaba.alink.operator.stream.evaluation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.params.evaluation.EvalBinaryClassStreamParams;

/**
 * Calculate the evaluation data within time windows for binary classifiction.
 * You can either give label column and predResult column or give label column and predDetail column.
 * Once predDetail column is given, the predResult column is ignored.
 * PositiveValue is optional, if given, it will placed at the first position in the output label Array.
 * If not given, the labels are sorted in descending order.
 */
@ParamSelectColumnSpec(name = "labelCol")
@ParamSelectColumnSpec(name = "predictionDetailCol", allowedTypeCollections = TypeCollections.STRING_TYPES)
@ParamSelectColumnSpec(name = "predictionCol")
@NameCn("二分类评估")
@NameEn("Evaluation for binary classes")
public class EvalBinaryClassStreamOp extends BaseEvalClassStreamOp <EvalBinaryClassStreamOp> implements
	EvalBinaryClassStreamParams <EvalBinaryClassStreamOp> {

	private static final long serialVersionUID = 3233575135152494606L;

	public EvalBinaryClassStreamOp() {
		this(null);
	}

	public EvalBinaryClassStreamOp(Params params) {
		super(params, true);
	}
}
