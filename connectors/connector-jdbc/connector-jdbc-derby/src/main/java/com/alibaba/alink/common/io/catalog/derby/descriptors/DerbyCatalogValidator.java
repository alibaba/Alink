package com.alibaba.alink.common.io.catalog.derby.descriptors;

import org.apache.flink.table.descriptors.CatalogDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;

public class DerbyCatalogValidator extends CatalogDescriptorValidator {
	public static final String CATALOG_TYPE_VALUE_DERBY = "derby";
	public static final String CATALOG_DERBY_PATH = "derbyPath";
	public static final String CATALOG_DERBY_USERNAME = "userName";
	public static final String CATALOG_DERBY_PASSWORD = "password";

	@Override
	public void validate(DescriptorProperties properties) {
		super.validate(properties);
		properties.validateValue(CATALOG_TYPE, CATALOG_TYPE_VALUE_DERBY, false);
		properties.validateString(CATALOG_DERBY_PATH, false, 1);
		properties.validateString(CATALOG_DERBY_USERNAME, true, 1);
		properties.validateString(CATALOG_DERBY_PASSWORD, true, 1);
	}
}
