package com.alibaba.alink.operator.batch.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.tree.predictors.RandomForestModelMapper;
import com.alibaba.alink.params.regression.CartRegPredictParams;

/**
 * The batch operator that predict the data using the cart regression model.
 */
public final class CartRegPredictBatchOp extends ModelMapBatchOp <CartRegPredictBatchOp> implements
	CartRegPredictParams <CartRegPredictBatchOp> {
	private static final long serialVersionUID = 8351046637860036501L;

	public CartRegPredictBatchOp() {
		this(null);
	}

	public CartRegPredictBatchOp(Params params) {
		super(RandomForestModelMapper::new, params);
	}
}
