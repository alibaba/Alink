package com.alibaba.alink.pipeline.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.tree.predictors.RandomForestModelMapper;
import com.alibaba.alink.params.classification.CartPredictParams;
import com.alibaba.alink.pipeline.MapModel;


/**
 * the cart model for pipeline.
 */
@NameCn("CART决策树分类模型")
public class CartModel extends MapModel <CartModel>
	implements CartPredictParams <CartModel> {

	private static final long serialVersionUID = 4574522822615498342L;

	public CartModel() {this(null);}

	public CartModel(Params params) {
		super(RandomForestModelMapper::new, params);
	}

}
