package com.alibaba.alink.operator.batch.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.batch.utils.ModelMapBatchOp;
import com.alibaba.alink.operator.common.tree.predictors.RandomForestModelMapper;
import com.alibaba.alink.params.classification.CartPredictParams;

/**
 * The batch operator that predict the data using the cart model.
 */
@NameCn("CART决策树分类预测")
public final class CartPredictBatchOp extends ModelMapBatchOp <CartPredictBatchOp> implements
	CartPredictParams <CartPredictBatchOp> {
	private static final long serialVersionUID = 2672681021424392380L;

	public CartPredictBatchOp() {
		this(null);
	}

	public CartPredictBatchOp(Params params) {
		super(RandomForestModelMapper::new, params);
	}
}
