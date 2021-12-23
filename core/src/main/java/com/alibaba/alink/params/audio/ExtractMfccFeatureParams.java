package com.alibaba.alink.params.audio;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.params.mapper.SISOMapperParams;

public interface ExtractMfccFeatureParams<T> extends
	SISOMapperParams <T>,
	HasSampleRate <T> {

	/**
	 * @cn-name 一个窗口的时间
	 * @cn 一个窗口的时间
	 */
	ParamInfo <Double> WINDOW_TIME = ParamInfoFactory
		.createParamInfo("windowSecond", Double.class)
		.setDescription("frame length for windowing data, in second")
		.setHasDefaultValue(0.128)
		.build();

	/**
	 * @cn-name 相邻窗口时间间隔
	 * @cn 相邻窗口时间间隔
	 */
	ParamInfo <Double> HOP_TIME = ParamInfoFactory
		.createParamInfo("hopSecond", Double.class)
		.setDescription("move appropriate seconds to next window")
		.setHasDefaultValue(0.032)
		.build();
	/**
	 * @cn-name mfcc参数
	 * @cn mfcc参数
	 */
	ParamInfo <Integer> NUM_MFCC = ParamInfoFactory
		.createParamInfo("nMfcc", Integer.class)
		.setDescription("number of MFCCs to return")
		.setHasDefaultValue(128)
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
