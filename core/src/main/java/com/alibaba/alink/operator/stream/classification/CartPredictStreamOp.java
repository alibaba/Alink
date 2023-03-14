package com.alibaba.alink.operator.stream.classification;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.util.function.TriFunction;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.common.tree.predictors.RandomForestModelMapper;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;
import com.alibaba.alink.params.classification.CartPredictParams;

/**
 * The stream operator that predict the data using the cart model.
 */
@NameCn("CART决策树分类预测")
@NameEn("CART Decision Tree Prediction")
public final class CartPredictStreamOp extends ModelMapStreamOp <CartPredictStreamOp>
	implements CartPredictParams <CartPredictStreamOp> {
	private static final long serialVersionUID = -7038751286088808725L;

	public CartPredictStreamOp() {
		this(new Params());
	}

	public CartPredictStreamOp(Params params) {
		super(RandomForestModelMapper::new, params);
	}

	public CartPredictStreamOp(BatchOperator model) {
		this(model, null);
	}

	public CartPredictStreamOp(BatchOperator model, Params params) {
		super(model, RandomForestModelMapper::new, params);
	}
}
