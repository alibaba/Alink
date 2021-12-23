package com.alibaba.alink.params.io.shared;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasCatalogName<T> extends WithParams <T> {
	/**
	 * @cn catalog名字
	 */
	ParamInfo <String> CATALOG_NAME = ParamInfoFactory
		.createParamInfo("catalogName", String.class)
		.setDescription("name of the catalog")
		.setHasDefaultValue(null)
		.build();

	default String getCatalogName() {
		return get(CATALOG_NAME);
	}

	default T setCatalogName(String value) {
		return set(CATALOG_NAME, value);
	}
}
