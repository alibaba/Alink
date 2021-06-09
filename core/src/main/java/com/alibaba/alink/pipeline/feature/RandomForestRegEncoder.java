package com.alibaba.alink.pipeline.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.RandomForestTrainBatchOp;
import com.alibaba.alink.params.feature.RandomForestRegEncoderParams;
import com.alibaba.alink.params.regression.RandomForestRegTrainParams;
import com.alibaba.alink.pipeline.Trainer;

public class RandomForestRegEncoder
	extends Trainer <RandomForestRegEncoder, RandomForestRegEncoderModel> implements
	RandomForestRegTrainParams <RandomForestRegEncoder>,
	RandomForestRegEncoderParams <RandomForestRegEncoder> {

	private static final long serialVersionUID = -8464158472165037937L;

	public RandomForestRegEncoder() {
		this(new Params());
	}

	public RandomForestRegEncoder(Params params) {
		super(params);
	}

	@Override
	protected BatchOperator <?> train(BatchOperator <?> in) {
		return new RandomForestTrainBatchOp(this.getParams()).linkFrom(in);
	}
}