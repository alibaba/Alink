package com.alibaba.alink.pipeline.feature;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.feature.RandomForestRegEncoderParams;
import com.alibaba.alink.params.regression.RandomForestRegTrainParams;
import com.alibaba.alink.pipeline.Trainer;

@NameCn("随机森林回归编码")
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

}
