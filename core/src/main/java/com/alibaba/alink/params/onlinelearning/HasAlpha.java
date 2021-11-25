package com.alibaba.alink.params.onlinelearning;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasAlpha<T> extends WithParams <T> {
	/**
	 * @cn-name 希腊字母：阿尔法
	 * @cn 经常用来表示算法特殊的参数
	 */
	ParamInfo <Double> ALPHA = ParamInfoFactory
		.createParamInfo("alpha", Double.class)
		.setDescription("alpha")
		.setHasDefaultValue(0.1)
		.build();

	default Double getAlpha() {return get(ALPHA);}

	default T setAlpha(Double value) {return set(ALPHA, value);}
}
