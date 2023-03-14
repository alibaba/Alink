package com.alibaba.alink.common.io.catalog.mysql.descriptors;

import org.apache.flink.table.descriptors.CatalogDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;

public class MySqlCatalogValidator extends CatalogDescriptorValidator {
	public static final String CATALOG_TYPE_VALUE_MYSQL = "mysql";
	public static final String CATALOG_MYSQL_URL = "mysqlUrl";
	public static final String CATALOG_MYSQL_PORT = "port";
	public static final String CATALOG_MYSQL_USERNAME = "userName";
	public static final String CATALOG_MYSQL_PASSWORD = "password";

	@Override
	public void validate(DescriptorProperties properties) {
		super.validate(properties);

		properties.validateValue(CATALOG_TYPE, CATALOG_TYPE_VALUE_MYSQL, false);
		properties.validateString(CATALOG_MYSQL_URL, false, 1);
		properties.validateString(CATALOG_MYSQL_PORT, true, 1);
		properties.validateString(CATALOG_MYSQL_USERNAME, true, 1);
		properties.validateString(CATALOG_MYSQL_PASSWORD, true, 1);
	}
}
