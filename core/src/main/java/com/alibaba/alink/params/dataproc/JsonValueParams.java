package com.alibaba.alink.params.dataproc;

import com.alibaba.alink.params.shared.colname.HasOutputCols;
import com.alibaba.alink.params.shared.colname.HasReservedCols;
import com.alibaba.alink.params.shared.colname.HasSelectedCol;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * Parameters for json value.
 * @param <T>
 */
public interface JsonValueParams<T> extends WithParams<T>,
	HasSelectedCol <T>,
	HasReservedCols <T>,
	HasOutputCols <T> {

	ParamInfo <String[]> JSON_PATHS = ParamInfoFactory
		.createParamInfo("jsonPath", String[].class)
		.setDescription(" json path")
		.setRequired()
		.setAlias(new String[] {"JsonPath"})
		.build();
	ParamInfo <Boolean> SKIP_FAILED = ParamInfoFactory
		.createParamInfo("skipFailed", boolean.class)
		.setDescription(" skip Failed")
		.setHasDefaultValue(false)
		.setAlias(new String[] {"skipFailed"})
		.build();

	default String[] getJsonPath() {
		return get(JSON_PATHS);
	}

	default T setJsonPath(String[] value) {
		return set(JSON_PATHS, value);
	}

	default boolean getSkipFailed() {
		return get(SKIP_FAILED);
	}

	default T setSkipFailed(boolean value) {
		return set(SKIP_FAILED, value);
	}

}
