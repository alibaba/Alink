package com.alibaba.alink.pipeline.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.regression.CartRegTrainBatchOp;
import com.alibaba.alink.params.feature.CartRegEncoderParams;
import com.alibaba.alink.params.regression.CartRegTrainParams;
import com.alibaba.alink.pipeline.Trainer;

@NameCn("Cart回归编码")
public class CartRegEncoder extends Trainer <CartRegEncoder, CartRegEncoderModel> implements
	CartRegTrainParams <CartRegEncoder>,
	CartRegEncoderParams <CartRegEncoder> {

	private static final long serialVersionUID = -8593371277511217184L;

	public CartRegEncoder() {
	}

	public CartRegEncoder(Params params) {
		super(params);
	}

	@Override
	protected BatchOperator <?> train(BatchOperator <?> in) {
		return new CartRegTrainBatchOp(this.getParams()).linkFrom(in);
	}
}
