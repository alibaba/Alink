package com.alibaba.alink.params.feature.featuregenerator;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasWindowTime<T> extends WithParams <T> {

	@NameCn("窗口大小")
	@DescCn("窗口大小")
	ParamInfo <Double> WINDOW_TIME = ParamInfoFactory
		.createParamInfo("windowTime", Double.class)
		.setDescription("window time interval")
		.setRequired()
		.build();

	default Double getWindowTime() {return get(WINDOW_TIME);}

	default T setWindowTime(Double value) {return set(WINDOW_TIME, value);}

	default T setWindowTime(Integer value) {return set(WINDOW_TIME, (double) value);}

}
