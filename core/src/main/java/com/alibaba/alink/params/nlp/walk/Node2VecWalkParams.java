package com.alibaba.alink.params.nlp.walk;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface Node2VecWalkParams<T> extends WithParams <T>,
	BaseWalkParams <T>, HasSamplingMethod<T> {

	/**
	 * @cn-name 算法参数P
	 * @cn 控制随机游走序列的跳转概率
	 */
	ParamInfo <Double> P = ParamInfoFactory
		.createParamInfo("p", Double.class)
		.setDescription("p")
		.setHasDefaultValue(1.)
		.build();
	/**
	 * @cn-name 算法参数Q
	 * @cn 控制随机游走序列的跳转概率
	 */
	ParamInfo <Double> Q = ParamInfoFactory
		.createParamInfo("q", Double.class)
		.setDescription("q")
		.setHasDefaultValue(1.)
		.build();

	default Double getP() {return get(P);}

	default T setP(Double value) {return set(P, value);}

	default Double getQ() {return get(Q);}

	default T setQ(Double value) {return set(Q, value);}

}
