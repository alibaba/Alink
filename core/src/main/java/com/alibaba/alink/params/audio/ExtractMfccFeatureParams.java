package com.alibaba.alink.params.audio;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.params.mapper.SISOMapperParams;

public interface ExtractMfccFeatureParams<T> extends
	SISOMapperParams <T>,
	HasSampleRate <T> {


	ParamInfo <Double> WINDOW_TIME = ParamInfoFactory
		.createParamInfo("windowSecond", Double.class)
		.setDescription("frame length for windowing data, in second")
		.setHasDefaultValue(0.025)
		.build();


	ParamInfo <Double> HOP_TIME = ParamInfoFactory
		.createParamInfo("hopSecond", Double.class)
		.setDescription("move appropriate seconds to next window")
		.setHasDefaultValue(0.01)
		.build();

	ParamInfo <Integer> NUM_MFCC = ParamInfoFactory
		.createParamInfo("nMfcc", Integer.class)
		.setDescription("number of MFCCs to return")
		.setHasDefaultValue(34)
		.build();

	default double getWindowTime() {
		return get(WINDOW_TIME);
	}

	default T setWindowTime(double value) {
		return set(WINDOW_TIME, value);
	}

	default double getHopTime() {
		return get(HOP_TIME);
	}

	default T setHopTime(double value) {
		return set(HOP_TIME, value);
	}

	default Integer getNumMfcc() {
		return get(NUM_MFCC);
	}

	default T setNumMfcc(Integer value) {
		return set(NUM_MFCC, value);
	}
}
