package com.alibaba.alink.params.feature.featuregenerator;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HopTimeWindowParams<T> extends
	GroupTimeWindowParams <T>,
	HasWindowTime <T> {

	@NameCn("滑动窗口大小")
	@DescCn("滑动窗口大小")
	ParamInfo <Double> HOP_TIME = ParamInfoFactory
		.createParamInfo("hopTime", Double.class)
		.setDescription("hop time interval")
		.setRequired()
		.build();

	default Double getHopTime() {return get(HOP_TIME);}

	default T setHopTime(Double value) {return set(HOP_TIME, value);}

	default T setHopTime(Integer value) {return set(HOP_TIME, (double) value);}
}
