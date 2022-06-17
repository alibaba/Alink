package com.alibaba.alink.common.io.catalog.sqlite.descriptors;

import org.apache.flink.table.descriptors.CatalogDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;

public class SqliteCatalogValidator extends CatalogDescriptorValidator {
	public static final String CATALOG_TYPE_VALUE_SQLITE = "sqlite";
	public static final String CATALOG_SQLITE_URLS = "dbUrls";
	public static final String CATALOG_SQLITE_USERNAME = "userName";
	public static final String CATALOG_SQLITE_PASSWORD = "password";

	@Override
	public void validate(DescriptorProperties properties) {
		super.validate(properties);
		properties.validateValue(CATALOG_TYPE, CATALOG_TYPE_VALUE_SQLITE, false);
		properties.validateString(CATALOG_SQLITE_URLS, false, 1);
		properties.validateString(CATALOG_SQLITE_USERNAME, true, 1);
		properties.validateString(CATALOG_SQLITE_PASSWORD, true, 1);
	}
}
