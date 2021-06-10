package com.alibaba.alink.common.io.catalog.sqlite.factories;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.catalog.CommonCatalogOptions;

public class SqliteCatalogFactoryOptions {

	public static final String IDENTIFIER = "sqlite";

	public static final ConfigOption <String> DEFAULT_DATABASE =
		ConfigOptions.key(CommonCatalogOptions.DEFAULT_DATABASE_KEY)
			.stringType()
			.defaultValue(null);

	public static final ConfigOption <String> URLS =
		ConfigOptions.key("dbUrls").stringType().noDefaultValue();

	public static final ConfigOption <String> USERNAME =
		ConfigOptions.key("userName").stringType().defaultValue(null);

	public static final ConfigOption <String> PASSWORD =
		ConfigOptions.key("password").stringType().defaultValue(null);

	private SqliteCatalogFactoryOptions() {
	}
}
