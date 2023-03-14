package com.alibaba.alink.pipeline.classification;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.lazy.HasLazyPrintModelInfo;
import com.alibaba.alink.params.classification.CartPredictParams;
import com.alibaba.alink.params.classification.CartTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * The pipeline for cart model.
 */
@NameCn("CART决策树分类")
public class Cart extends Trainer <Cart, CartModel> implements
	CartTrainParams <Cart>,
	CartPredictParams <Cart>, HasLazyPrintModelInfo <Cart> {

	private static final long serialVersionUID = 8684056184808161658L;

	public Cart() {
		super();
	}

	public Cart(Params params) {
		super(params);
	}

}
