package com.alibaba.alink.operator.stream.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.tree.predictors.RandomForestModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.regression.CartRegPredictParams;

/**
 * The stream operator that predict the data using the cart model.
 */
@NameCn("CART决策树回归预测")
public final class CartRegPredictStreamOp extends ModelMapStreamOp <CartRegPredictStreamOp>
	implements CartRegPredictParams <CartRegPredictStreamOp> {
	private static final long serialVersionUID = 4303341187252058311L;

	public CartRegPredictStreamOp(BatchOperator model) {
		this(model, null);
	}

	public CartRegPredictStreamOp(BatchOperator model, Params params) {
		super(model, RandomForestModelMapper::new, params);
	}
}
