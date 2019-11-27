package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;
import com.alibaba.alink.params.io.shared_params.HasOutputTableName;

public interface DerbySinkParams<T> extends WithParams<T>,
	DerbyDBParams <T>,
	HasOutputTableName <T> {


	ParamInfo<Boolean> OVERWRITE_SINK = ParamInfoFactory
		.createParamInfo("overwriteSink", Boolean.class)
		.setDescription("Whether to overwrite existing data.")
		.setHasDefaultValue(false)
		.build();

	default Boolean getOverwriteSink() {
		return get(OVERWRITE_SINK);
	}

	default T setOverwriteSink(Boolean value) {
		return set(OVERWRITE_SINK, value);
	}
}
