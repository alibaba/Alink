package com.alibaba.alink.params.feature.featuregenerator;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasPrecedingRows <T> extends WithParams<T> {

	/**
	 * @cn-name 数据窗口大小
	 * @cn 数据窗口大小
	 */
	ParamInfo <Integer> PRECEDING_ROWS = ParamInfoFactory
		.createParamInfo("precedingRows", Integer.class)
		.setDescription("rows of window")
		.setHasDefaultValue(null)
		.build();

	default Integer getPrecedingRows() {return get(PRECEDING_ROWS);}

	default T setPrecedingRows(Integer value) {return set(PRECEDING_ROWS, value);}

}
