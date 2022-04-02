package com.alibaba.alink.params.timeseries;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface ShiftParams<T> extends
	TimeSeriesPredictParams <T> {

	@NameCn("shift个数")
	@DescCn("shift个数")
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
