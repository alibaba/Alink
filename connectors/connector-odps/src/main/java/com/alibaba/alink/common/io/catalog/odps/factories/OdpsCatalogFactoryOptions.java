package com.alibaba.alink.common.io.catalog.odps.factories;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.catalog.CommonCatalogOptions;

public class OdpsCatalogFactoryOptions {

	public static final String IDENTIFIER = "odps";

	public static final ConfigOption <String> DEFAULT_DATABASE =
		ConfigOptions.key(CommonCatalogOptions.DEFAULT_DATABASE_KEY)
			.stringType()
			.defaultValue(null);

	public static final ConfigOption <String> ACCESS_ID =
		ConfigOptions.key("accessId").stringType().noDefaultValue();

	public static final ConfigOption <String> ACCESS_KEY =
		ConfigOptions.key("accessKey").stringType().noDefaultValue();

	public static final ConfigOption <String> PROJECT =
		ConfigOptions.key("project").stringType().noDefaultValue();

	public static final ConfigOption <String> ENDPOINT =
		ConfigOptions.key("endpoint").stringType().noDefaultValue();

	public static final ConfigOption <String> RUNNING_PROJECT =
		ConfigOptions.key("runningProject").stringType().defaultValue(null);

	private OdpsCatalogFactoryOptions() {
	}
}
