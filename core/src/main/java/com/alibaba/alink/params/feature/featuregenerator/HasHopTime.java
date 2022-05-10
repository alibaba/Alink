package com.alibaba.alink.params.feature.featuregenerator;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasHopTime<T> extends WithParams <T> {

	@NameCn("滑动窗口大小")
	@DescCn("滑动窗口大小")
	ParamInfo <String> HOP_TIME = ParamInfoFactory
		.createParamInfo("hopTime", String.class)
		.setDescription("hop time interval")
		.setRequired()
		.build();

	default String getHopTime() {return get(HOP_TIME);}

	default T setHopTime(Double value) {return set(HOP_TIME, value.toString());}

	default T setHopTime(Integer value) {return set(HOP_TIME, value.toString());}

	default T setHopTime(String value) {return set(HOP_TIME, value);}

}
