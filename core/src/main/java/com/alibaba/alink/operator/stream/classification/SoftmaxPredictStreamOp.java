package com.alibaba.alink.operator.stream.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.linear.SoftmaxModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.classification.SoftmaxPredictParams;

/**
 * Softmax predict stream operator. this operator predict data's label with linear model.
 */
@ParamSelectColumnSpec(name = "vectorCol",
	allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@NameCn("Softmax预测")
public final class SoftmaxPredictStreamOp extends ModelMapStreamOp <SoftmaxPredictStreamOp>
	implements SoftmaxPredictParams <SoftmaxPredictStreamOp> {

	private static final long serialVersionUID = -5703624582223381415L;

	public SoftmaxPredictStreamOp(BatchOperator model) {
		this(model, null);
	}

	public SoftmaxPredictStreamOp(BatchOperator model, Params params) {
		super(model, SoftmaxModelMapper::new, params);
	}

}
