package com.alibaba.alink.params.timeseries;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.params.ParamUtil;

public interface HasArimaGarchMethod<T> extends WithParams <T> {

	/**
	 * @cn-name arima garch方法
	 * @cn arima garch方法
	 */
	ParamInfo <ArimaGarchMethod> ARIMA_GARCH_METHOD = ParamInfoFactory
		.createParamInfo("arimaGarchMethod", ArimaGarchMethod.class)
		.setDescription("arima garch method")
		.setHasDefaultValue(ArimaGarchMethod.CONSIST)
		.build();

	default ArimaGarchMethod getArimaGarchMethod() {
		return get(ARIMA_GARCH_METHOD);
	}

	default T setArimaGarchMethod(ArimaGarchMethod value) {
		return set(ARIMA_GARCH_METHOD, value);
	}

	default T setArimaGarchMethod(String value) {
		return set(ARIMA_GARCH_METHOD, ParamUtil.searchEnum(ARIMA_GARCH_METHOD, value));
	}

	enum ArimaGarchMethod {
		CONSIST,
		SEPARATE
	}
}
