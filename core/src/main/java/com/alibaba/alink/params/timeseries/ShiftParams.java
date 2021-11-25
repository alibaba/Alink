package com.alibaba.alink.params.timeseries;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

public interface ShiftParams<T> extends
	TimeSeriesPredictParams <T> {

	/**
	 * @cn-name shift个数
	 * @cn shift个数
	 */
	ParamInfo <Integer> SHIFT_NUM = ParamInfoFactory
		.createParamInfo("shiftNum", Integer.class)
		.setDescription("shift number")
		.setHasDefaultValue(7)
		.build();

	default Integer getShiftNum() {
		return get(SHIFT_NUM);
	}

	default T setShiftNum(Integer value) {
		return set(SHIFT_NUM, value);
	}

}
