package com.alibaba.alink.pipeline.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.RandomForestTrainBatchOp;
import com.alibaba.alink.params.classification.RandomForestTrainParams;
import com.alibaba.alink.params.feature.RandomForestEncoderParams;
import com.alibaba.alink.pipeline.Trainer;

@NameCn("随机森林编码")
public class RandomForestEncoder
	extends Trainer <RandomForestEncoder, RandomForestEncoderModel> implements
	RandomForestTrainParams <RandomForestEncoder>,
	RandomForestEncoderParams <RandomForestEncoder> {

	private static final long serialVersionUID = -8464158472165037937L;

	public RandomForestEncoder() {
		this(new Params());
	}

	public RandomForestEncoder(Params params) {
		super(params);
	}

	@Override
	protected BatchOperator <?> train(BatchOperator <?> in) {
		return new RandomForestTrainBatchOp(this.getParams()).linkFrom(in);
	}
}
