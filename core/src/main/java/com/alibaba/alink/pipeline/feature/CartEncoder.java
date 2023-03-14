package com.alibaba.alink.pipeline.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.classification.CartTrainParams;
import com.alibaba.alink.params.feature.CartEncoderParams;
import com.alibaba.alink.pipeline.Trainer;

@NameCn("Cart编码")
public class CartEncoder extends Trainer <CartEncoder, CartEncoderModel> implements
	CartTrainParams <CartEncoder>,
	CartEncoderParams <CartEncoder> {

	private static final long serialVersionUID = -8593371277511217184L;

	public CartEncoder() {
	}

	public CartEncoder(Params params) {
		super(params);
	}

}
