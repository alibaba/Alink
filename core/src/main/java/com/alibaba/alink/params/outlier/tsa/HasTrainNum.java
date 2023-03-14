package com.alibaba.alink.params.outlier.tsa;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

//this is required in predict situation, including zScore, shortMoM and stream.
public interface HasTrainNum<T> extends WithParams <T> {
	ParamInfo <Integer> TRAIN_NUM = ParamInfoFactory
		.createParamInfo("trainNum", Integer.class)
		.setDescription("the train num")
		.setRequired()
		.build();

	default Integer getTrainNum() {
		return get(TRAIN_NUM);
	}

	default T setTrainNum(Integer value) {
		return set(TRAIN_NUM, value);
	}
}
