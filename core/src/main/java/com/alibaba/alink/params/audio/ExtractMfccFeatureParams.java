package com.alibaba.alink.params.audio;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.mapper.SISOMapperParams;

public interface ExtractMfccFeatureParams<T> extends
	SISOMapperParams <T>,
	HasSampleRate <T> {

	@NameCn("一个窗口的时间")
	@DescCn("一个窗口的时间")
	ParamInfo <Double> WINDOW_TIME = ParamInfoFactory
		.createParamInfo("windowTime", Double.class)
		.setDescription("frame length for windowing data, in second")
		.setHasDefaultValue(0.128)
		.build();

	@NameCn("相邻窗口时间间隔")
	@DescCn("相邻窗口时间间隔")
	ParamInfo <Double> HOP_TIME = ParamInfoFactory
		.createParamInfo("hopTime", Double.class)
		.setDescription("move appropriate seconds to next window")
		.setHasDefaultValue(0.032)
		.build();
	@NameCn("mfcc参数")
	@DescCn("mfcc参数")
	ParamInfo <Integer> NUM_MFCC = ParamInfoFactory
		.createParamInfo("numMfcc", Integer.class)
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
