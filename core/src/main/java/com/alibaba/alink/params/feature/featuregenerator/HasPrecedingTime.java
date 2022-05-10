package com.alibaba.alink.params.feature.featuregenerator;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasPrecedingTime<T> extends WithParams <T> {

	@NameCn("时间窗口大小")
	@DescCn("时间窗口大小")
	ParamInfo <String> PRECEDING_TIME = ParamInfoFactory
		.createParamInfo("precedingTime", String.class)
		.setDescription("time interval")
		.setRequired()
		.build();

	default String getPrecedingTime() {return get(PRECEDING_TIME);}

	default T setPrecedingTime(double value) {return set(PRECEDING_TIME, String.valueOf(value));}

	default T setPrecedingTime(int value) {return set(PRECEDING_TIME, String.valueOf(value));}

	default T setPrecedingTime(String value) {return set(PRECEDING_TIME, value);}

}
