package com.alibaba.alink.operator.stream.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.linear.LinearModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.classification.LogisticRegressionPredictParams;

/**
 * Linear logistic regression predict stream operator. this operator predict data's label with linear model.
 */
@ParamSelectColumnSpec(name = "vectorCol",
	allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@NameCn("逻辑回归预测")
public final class LogisticRegressionPredictStreamOp extends ModelMapStreamOp <LogisticRegressionPredictStreamOp>
	implements LogisticRegressionPredictParams <LogisticRegressionPredictStreamOp> {

	private static final long serialVersionUID = 7364058085791363663L;

	public LogisticRegressionPredictStreamOp() {
		super(LinearModelMapper::new, new Params());
	}

	public LogisticRegressionPredictStreamOp(Params params) {
		super(LinearModelMapper::new, params);
	}

	public LogisticRegressionPredictStreamOp(BatchOperator model) {
		this(model, new Params());
	}

	public LogisticRegressionPredictStreamOp(BatchOperator model, Params params) {
		super(model, LinearModelMapper::new, params);
	}
}



