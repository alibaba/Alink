package com.alibaba.alink.common.viz;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface AlinkViz<T> extends WithParams <T> {

	ParamInfo <String> VIZ_NAME = ParamInfoFactory
		.createParamInfo("vizName", String.class)
		.setDescription("Name of Visualization")
		.setOptional()
		.build();

	default VizDataWriterInterface getVizDataWriter() {
		return ScreenManager.getScreenManager().getVizDataWriter(getParams());
	}

	default String getVizName() {
		return getParams().get(VIZ_NAME);
	}

	default T setVizName(String value) {
		return set(VIZ_NAME, value);
	}
}
