package com.alibaba.alink.params.regression;

import com.alibaba.alink.params.shared.linear.LinearTrainParams;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

/**
 * parameters of svr train process.
 *
 */
public interface LinearSvrTrainParams<T> extends
	LinearTrainParams <T> {

	ParamInfo <Double> C = ParamInfoFactory
		.createParamInfo("C", Double.class)
		.setDescription("punish factor.")
		.setRequired()
		.build();

	ParamInfo <Double> TAU = ParamInfoFactory
		.createParamInfo("tau", Double.class)
		.setDescription("width of the pipe in svr algo. default value is 0.1.")
		.setHasDefaultValue(0.1)
		.build();

	default Double getC() {
		return get(C);
	}

	default T setC(Double value) {
		return set(C, value);
	}

	default Double getTau() {
		return get(TAU);
	}

	default T setTau(Double value) {
		return set(TAU, value);
	}

}
