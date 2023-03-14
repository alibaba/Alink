package com.alibaba.alink.operator.stream.evaluation;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.params.evaluation.EvalMultiClassStreamParams;

/**
 * Calculate the evaluation data within time windows for multi classifiction.
 * You can either give label column and predResult column or give label column and predDetail column.
 * Once predDetail column is given, the predResult column is ignored.
 * The labels are sorted in descending order in the output label array and confusion matrix..
 */
@ParamSelectColumnSpec(name = "labelCol")
@ParamSelectColumnSpec(name = "predictionCol")
@NameCn("多分类评估")
@NameEn("Evaluation for multiple classes")
public class EvalMultiClassStreamOp extends BaseEvalClassStreamOp <EvalMultiClassStreamOp> implements
	EvalMultiClassStreamParams <EvalMultiClassStreamOp> {

	private static final long serialVersionUID = 630790966242264629L;

	public EvalMultiClassStreamOp() {
		this(null);
	}

	public EvalMultiClassStreamOp(Params params) {
		super(params, false);
	}
}
