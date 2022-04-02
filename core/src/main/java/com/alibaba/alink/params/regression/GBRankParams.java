package com.alibaba.alink.params.regression;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface GBRankParams<T> extends
	LambdaMartNdcgParams <T> {

	@NameCn("tau")
	@DescCn("the coef of label diff.")
	ParamInfo <Double> TAU = ParamInfoFactory
		.createParamInfo("tau", Double.class)
		.setHasDefaultValue(0.6)
		.build();

	@NameCn("p")
	@DescCn("the reference will be pow by p.")
	ParamInfo <Double> P = ParamInfoFactory
		.createParamInfo("p", Double.class)
		.setHasDefaultValue(1.0)
		.build();

	default T setTau(Double value) {
		return set(TAU, value);
	}

	default Double getTau() {
		return get(TAU);
	}

	default T setP(Double value) {
		return set(P, value);
	}

	default Double getP() {
		return get(P);
	}
}

