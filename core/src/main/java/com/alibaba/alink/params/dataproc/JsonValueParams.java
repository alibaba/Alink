package com.alibaba.alink.params.dataproc;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.params.mapper.MapperParams;
import com.alibaba.alink.params.shared.colname.HasOutputColTypesDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasOutputCols;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasSelectedCol;

/**
 * Parameters for json value.
 *
 * @param <T>
 */
public interface JsonValueParams<T> extends
	MapperParams <T>,
	HasSelectedCol <T>,
	HasReservedColsDefaultAsNull <T>,
	HasOutputCols <T>,
	HasOutputColTypesDefaultAsNull <T> {

	/**
	 * Param "JsonPath"
	 *
	 * @cn-name Json 路径数组
	 * @cn 用来指定 Json 抽取的内容。
	 */
	ParamInfo <String[]> JSON_PATHS = ParamInfoFactory
		.createParamInfo("jsonPath", String[].class)
		.setDescription(" json path")
		.setRequired()
		.setAlias(new String[] {"JsonPath"})
		.build();
	/**
	 * Param "skipFailed"
	 *
	 * @cn-name 是否跳过错误
	 * @cn 当遇到抽取值为null 时是否跳过
	 */
	ParamInfo <Boolean> SKIP_FAILED = ParamInfoFactory
		.createParamInfo("skipFailed", boolean.class)
		.setDescription(" skip Failed")
		.setHasDefaultValue(false)
		.setAlias(new String[] {"skipFailed"})
		.build();

	default String[] getJsonPath() {
		return get(JSON_PATHS);
	}

	default T setJsonPath(String... value) {
		return set(JSON_PATHS, value);
	}

	default boolean getSkipFailed() {
		return get(SKIP_FAILED);
	}

	default T setSkipFailed(boolean value) {
		return set(SKIP_FAILED, value);
	}

}
