package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasModelTimeDefaultAsNull<T> extends WithParams <T> {

	/**
	 * @cn-name 批模型时间戳
	 * @cn 模型时间戳。默认当前时间。 使用yyyy-mm-dd hh:mm:ss.fffffffff格式，详见Timestamp.valueOf(String s)
	 */
	ParamInfo <String> MODEL_TIME = ParamInfoFactory
		.createParamInfo("modelTime", String.class)
		.setDescription("time of the batch model.")
		.setHasDefaultValue(null)
		.build();

	default String getModelTime() {return get(MODEL_TIME);}

	default T setModelTime(String value) {return set(MODEL_TIME, value);}
}
