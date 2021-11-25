package com.alibaba.alink.params.shared;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasTimeCol<T> extends WithParams <T> {

	/**
	 * @cn-name 时间戳列(毫秒)
	 * @cn 时间戳列(毫秒)
	 */
	ParamInfo <String> TIME_COL = ParamInfoFactory
		.createParamInfo("timeCol", String.class)
		.setAlias(new String[] {"timeColName"})
		.setDescription("time col name")
		.setRequired()
		.build();

	default String getTimeCol() {
		return get(TIME_COL);
	}

	default T setTimeCol(String value) {
		return set(TIME_COL, value);
	}

}
