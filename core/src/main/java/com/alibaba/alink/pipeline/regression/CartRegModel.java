package com.alibaba.alink.pipeline.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.tree.predictors.RandomForestModelMapper;
import com.alibaba.alink.params.regression.CartRegPredictParams;
import com.alibaba.alink.pipeline.MapModel;

/**
 * the cart regression model for pipeline.
 */
@NameCn("CART决策树回归模型")
public class CartRegModel extends MapModel <CartRegModel>
	implements CartRegPredictParams <CartRegModel> {

	private static final long serialVersionUID = 682444763102078997L;

	public CartRegModel() {this(null);}

	public CartRegModel(Params params) {
		super(RandomForestModelMapper::new, params);
	}

}
