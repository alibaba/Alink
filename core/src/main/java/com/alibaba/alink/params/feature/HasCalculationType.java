package com.alibaba.alink.params.feature;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.params.ParamUtil;

public interface HasCalculationType<T> extends WithParams <T> {
	/**
	 * @cn-name 计算类型
	 * @cn 计算类型，包含"CORR", "COV"两种。
	 */
	ParamInfo <CalculationType> CALCULATION_TYPE = ParamInfoFactory
		.createParamInfo("calculationType", CalculationType.class)
		.setDescription("compute type, be CORR, COV.")
		.setHasDefaultValue(CalculationType.CORR)
		.setAlias(new String[] {"calcType", "pcaType"})
		.build();

	default CalculationType getCalculationType() {
		return get(CALCULATION_TYPE);
	}

	default T setCalculationType(CalculationType value) {
		return set(CALCULATION_TYPE, value);
	}

	default T setCalculationType(String value) {
		return set(CALCULATION_TYPE, ParamUtil.searchEnum(CALCULATION_TYPE, value));
	}

	/**
	 * pca calculation type.
	 */
	enum CalculationType {
		/**
		 * Correlation
		 */
		CORR,
		/**
		 * Covariance
		 */
		COV
	}
}
