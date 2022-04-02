package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

public interface SqliteCatalogParams<T> extends JdbcCatalogParams <T> {
	/**
	 * @cn url地址, 表示多个 sqlite db的文件路径
	 */
	ParamInfo <String[]> URLS = ParamInfoFactory
		.createParamInfo("urls", String[].class)
		.setDescription("urls")
		.setRequired()
		.build();

	default String[] getUrls() {
		return get(URLS);
	}

	default T setUrls(String... value) {
		return set(URLS, value);
	}
}
