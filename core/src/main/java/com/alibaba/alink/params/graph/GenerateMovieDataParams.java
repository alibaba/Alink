package com.alibaba.alink.params.graph;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface GenerateMovieDataParams<T> extends WithParams <T> {

	ParamInfo <String[]> INPUT_COLS = ParamInfoFactory
		.createParamInfo("INPUT_COLS", String[].class)
		.setDescription("whether the graph is directed or not")
		.setRequired()
		.build();

	default String[] getInputCols() {
		return get(INPUT_COLS);
	}

	default T setInputCols(String[] value) {
		return set(INPUT_COLS, value);
	}

	ParamInfo <Double> MINI_RATE = ParamInfoFactory
		.createParamInfo("MINI_RATE", Double.class)
		.setDescription("whether the graph is directed or not")
		.setHasDefaultValue(3.5)
		.build();

	default Double getMiniRate() {
		return get(MINI_RATE);
	}

	default T setMiniRate(Double value) {
		return set(MINI_RATE, value);
	}

	ParamInfo <Integer> PATH_LEN = ParamInfoFactory
		.createParamInfo("PATH_LEN", Integer.class)
		.setDescription("whether the graph is directed or not")
		.setHasDefaultValue(2)
		.build();

	default Integer getPathLen() {
		return get(PATH_LEN);
	}

	default T setPathLen(Integer value) {
		return set(PATH_LEN, value);
	}
}
