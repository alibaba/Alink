package com.alibaba.alink.pipeline.regression;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.lazy.HasLazyPrintModelInfo;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.regression.CartRegTrainBatchOp;
import com.alibaba.alink.params.regression.CartRegPredictParams;
import com.alibaba.alink.params.regression.CartRegTrainParams;
import com.alibaba.alink.pipeline.Trainer;

/**
 * The pipeline for cart regression model.
 */
public class CartReg extends Trainer <CartReg, CartRegModel> implements
	CartRegTrainParams <CartReg>,
	CartRegPredictParams <CartReg>, HasLazyPrintModelInfo <CartReg> {

	private static final long serialVersionUID = -4524477437837089803L;

	public CartReg() {
		super();
	}

	public CartReg(Params params) {
		super(params);
	}

	@Override
	protected BatchOperator <?> train(BatchOperator <?> in) {
		return new CartRegTrainBatchOp(this.getParams()).linkFrom(in);
	}

}