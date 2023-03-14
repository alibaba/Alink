package com.alibaba.alink.params.outlier.tsa;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.params.validators.MinValidator;

public interface HasAnomsNum<T> extends WithParams <T> {
	ParamInfo <Integer> ANOMS_NUM = ParamInfoFactory
		.createParamInfo("anomsNum", Integer.class)
		.setDescription("The anomaly number.")
		.setHasDefaultValue(3)
		.setValidator(new MinValidator <>(0))
		.build();

	default Integer getAnomsNum() {
		return get(ANOMS_NUM);
	}

	default T setAnomsNum(Integer value) {
		return set(ANOMS_NUM, value);
	}
}
