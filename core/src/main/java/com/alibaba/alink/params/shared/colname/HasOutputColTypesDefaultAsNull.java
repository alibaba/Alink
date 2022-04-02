package com.alibaba.alink.params.shared.colname;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

/**
 * An interface for classes with a parameter specifying type of multiple output columns.
 */
public interface HasOutputColTypesDefaultAsNull<T> extends WithParams <T> {

	@NameCn("输出结果列列类型数组")
	@DescCn("输出结果列类型数组，必选")
	ParamInfo <String[]> OUTPUT_COL_TYPES = ParamInfoFactory
		.createParamInfo("outputColTypes", String[].class)
		.setDescription("Types of the output columns")
		.setHasDefaultValue(null)
		.build();

	default String[] getOutputColTypes() {
		return get(OUTPUT_COL_TYPES);
	}

	default T setOutputColTypes(String... colTypes) {
		return set(OUTPUT_COL_TYPES, colTypes);
	}
}
