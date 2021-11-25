package com.alibaba.alink.params.shared.clustering;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasHalfLife<T> extends WithParams <T> {

	/**
	 * @cn-name 半生命周期
	 * @cn 半生命周期
	 */
	ParamInfo <Integer> HALF_LIFE = ParamInfoFactory
		.createParamInfo("halfLife", Integer.class)
		.setDescription("half life")
		.setRequired()
		.build();

	default Integer getHalfLife() {return get(HALF_LIFE);}

	default T setHalfLife(Integer value) {return set(HALF_LIFE, value);}
}
