package com.alibaba.alink.operator.stream.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.linear.LinearModelMapper;
import com.alibaba.alink.operator.common.tree.predictors.GbdtModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.regression.LassoRegPredictParams;

/**
 * Lasso regression predict stream operator. this operator predict data's regression value with linear model.
 */
@ParamSelectColumnSpec(name = "vectorCol",
	allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@NameCn("Lasso回归预测")
public class LassoRegPredictStreamOp extends ModelMapStreamOp <LassoRegPredictStreamOp>
	implements LassoRegPredictParams <LassoRegPredictStreamOp> {

	private static final long serialVersionUID = -4399709004424148195L;

	public LassoRegPredictStreamOp() {
		super(LinearModelMapper::new, new Params());
	}

	public LassoRegPredictStreamOp(Params params) {
		super(LinearModelMapper::new, params);
	}

	public LassoRegPredictStreamOp(BatchOperator model) {
		super(model, LinearModelMapper::new, new Params());
	}

	public LassoRegPredictStreamOp(BatchOperator model, Params params) {
		super(model, LinearModelMapper::new, params);
	}

}
