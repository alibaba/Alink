package com.alibaba.alink.params.timeseries;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

public interface AutoArimaParams<T> extends
	TimeSeriesPredictParams <T>,
	AutoArimaAlgoParams <T> {

	/**
	 * @cn-name d
	 * @cn d
	 */
	ParamInfo<Integer> D = ParamInfoFactory
		.createParamInfo("d", Integer.class)
		.setDescription("d")
		.setHasDefaultValue(-1)
		.build();

	default Integer getD() {
		return get(D);
	}

	default T setD(Integer value) {
		return set(D, value);
	}
}

