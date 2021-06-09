package com.alibaba.alink.pipeline.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.CartTrainBatchOp;
import com.alibaba.alink.params.classification.CartTrainParams;
import com.alibaba.alink.params.feature.CartEncoderParams;
import com.alibaba.alink.pipeline.Trainer;

public class CartEncoder extends Trainer <CartEncoder, CartEncoderModel> implements
	CartTrainParams <CartEncoder>,
	CartEncoderParams <CartEncoder> {

	private static final long serialVersionUID = -8593371277511217184L;

	public CartEncoder() {
	}

	public CartEncoder(Params params) {
		super(params);
	}

	@Override
	protected BatchOperator <?> train(BatchOperator <?> in) {
		return new CartTrainBatchOp(this.getParams()).linkFrom(in);
	}
}
