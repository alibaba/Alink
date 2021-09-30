package com.alibaba.alink.params.audio;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasSampleRate<T> extends WithParams <T> {

	ParamInfo <Integer> SAMPLE_RATE = ParamInfoFactory
		.createParamInfo("sampleRate", Integer.class)
		.setDescription("sample rate of audio")
		.setRequired()
		.build();

	default Integer getSampleRate() {
		return get(SAMPLE_RATE);
	}

	default T setSampleRate(Integer value) {
		return set(SAMPLE_RATE, value);
	}

}
