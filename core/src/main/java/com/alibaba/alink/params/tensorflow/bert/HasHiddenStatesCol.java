package com.alibaba.alink.params.tensorflow.bert;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasHiddenStatesCol<T> extends WithParams <T> {

	ParamInfo <String> HIDDEN_STATES_COL = ParamInfoFactory
		.createParamInfo("hiddenState", String.class)
		.setDescription("Name of the hidden states column")
		.setHasDefaultValue("hidden_states")
		.build();

	default String getHiddenStatesCol() {
		return get(HIDDEN_STATES_COL);
	}

	default T setHiddenStatesCol(String colName) {
		return set(HIDDEN_STATES_COL, colName);
	}
}
