package com.alibaba.alink.params.timeseries;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.params.ParamUtil;

public interface HasIcType<T> extends WithParams <T> {


	ParamInfo <IcType> IC_TYPE = ParamInfoFactory
		.createParamInfo("icType", IcType.class)
		.setDescription("ic type")
		.setHasDefaultValue(IcType.AIC)
		.build();

	default IcType getIcType() {
		return get(IC_TYPE);
	}

	default T setIcType(IcType value) {
		return set(IC_TYPE, value);
	}

	default T setIcType(String value) {
		return set(IC_TYPE, ParamUtil.searchEnum(IC_TYPE, value));
	}

	enum IcType {
		AIC,
		BIC,
		HQIC
	}
}
