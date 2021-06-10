package com.alibaba.alink.common.io.catalog.mysql.factories;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.catalog.CommonCatalogOptions;

public class MySqlCatalogFactoryOptions {

	public static final String IDENTIFIER = "mysql";

	public static final ConfigOption <String> DEFAULT_DATABASE =
		ConfigOptions.key(CommonCatalogOptions.DEFAULT_DATABASE_KEY)
			.stringType()
			.defaultValue(null);

	public static final ConfigOption <String> URL =
		ConfigOptions.key("mysqlUrl").stringType().defaultValue(null);

	public static final ConfigOption <String> PORT =
		ConfigOptions.key("port").stringType().defaultValue(null);

	public static final ConfigOption <String> USERNAME =
		ConfigOptions.key("userName").stringType().defaultValue(null);

	public static final ConfigOption <String> PASSWORD =
		ConfigOptions.key("password").stringType().defaultValue(null);

	private MySqlCatalogFactoryOptions() {
	}
}
