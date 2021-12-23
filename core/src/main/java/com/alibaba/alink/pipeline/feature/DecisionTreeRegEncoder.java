package com.alibaba.alink.pipeline.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.regression.CartRegTrainBatchOp;
import com.alibaba.alink.params.classification.DecisionTreeTrainParams;
import com.alibaba.alink.params.feature.DecisionTreeRegEncoderParams;
import com.alibaba.alink.pipeline.Trainer;

public class DecisionTreeRegEncoder extends Trainer <DecisionTreeRegEncoder, DecisionTreeRegEncoderModel> implements
	DecisionTreeTrainParams <DecisionTreeRegEncoder>,
	DecisionTreeRegEncoderParams <DecisionTreeRegEncoder> {

	private static final long serialVersionUID = -8593371277511217184L;

	public DecisionTreeRegEncoder() {
	}

	public DecisionTreeRegEncoder(Params params) {
		super(params);
	}

	@Override
	protected BatchOperator <?> train(BatchOperator <?> in) {
		return new CartRegTrainBatchOp(this.getParams()).linkFrom(in);
	}
}
