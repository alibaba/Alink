package com.alibaba.alink.common.io.catalog.datahub.descriptors;

import org.apache.flink.table.descriptors.CatalogDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;

public class DataHubCatalogValidator extends CatalogDescriptorValidator {
	public static final String CATALOG_TYPE_VALUE_DATAHUB = "datahub";
	public static final String CATALOG_DATAHUB_ACCESS_ID = "accessId";
	public static final String CATALOG_DATAHUB_ACCESS_KEY = "accessKey";
	public static final String CATALOG_DATAHUB_PROJECT = "project";
	public static final String CATALOG_DATAHUB_ENDPOINT = "endpoint";

	@Override
	public void validate(DescriptorProperties properties) {
		super.validate(properties);
		properties.validateValue(CATALOG_TYPE, CATALOG_TYPE_VALUE_DATAHUB, false);
		properties.validateString(CATALOG_DATAHUB_ACCESS_ID, false, 1);
		properties.validateString(CATALOG_DATAHUB_ACCESS_KEY, false, 1);
		properties.validateString(CATALOG_DATAHUB_PROJECT, false, 1);
		properties.validateString(CATALOG_DATAHUB_ENDPOINT, false, 1);
	}
}
