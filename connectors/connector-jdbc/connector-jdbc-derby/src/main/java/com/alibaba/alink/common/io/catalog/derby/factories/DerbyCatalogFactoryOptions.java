package com.alibaba.alink.common.io.catalog.derby.factories;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.catalog.CommonCatalogOptions;

public class DerbyCatalogFactoryOptions {

	public static final String IDENTIFIER = "derby";

	public static final ConfigOption <String> DEFAULT_DATABASE =
		ConfigOptions.key(CommonCatalogOptions.DEFAULT_DATABASE_KEY)
			.stringType()
			.defaultValue(null);

	public static final ConfigOption <String> PATH =
		ConfigOptions.key("derbyPath").stringType().noDefaultValue();

	public static final ConfigOption <String> USERNAME =
		ConfigOptions.key("userName").stringType().defaultValue(null);

	public static final ConfigOption <String> PASSWORD =
		ConfigOptions.key("password").stringType().defaultValue(null);

	private DerbyCatalogFactoryOptions() {
	}
}
