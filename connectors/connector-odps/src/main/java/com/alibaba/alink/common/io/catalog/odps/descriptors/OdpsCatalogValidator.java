package com.alibaba.alink.common.io.catalog.odps.descriptors;

import org.apache.flink.table.descriptors.CatalogDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;

public class OdpsCatalogValidator extends CatalogDescriptorValidator {
	public static final String CATALOG_TYPE_VALUE_ODPS = "odps";
	public static final String CATALOG_ODPS_ACCESS_ID = "accessId";
	public static final String CATALOG_ODPS_ACCESS_KEY = "accessKey";
	public static final String CATALOG_ODPS_PROJECT = "project";
	public static final String CATALOG_ODPS_ENDPOINT = "endpoint";
	public static final String CATALOG_ODPS_RUNNING_PROJECT = "runningProject";

	@Override
	public void validate(DescriptorProperties properties) {
		super.validate(properties);
		properties.validateValue(CATALOG_TYPE, CATALOG_TYPE_VALUE_ODPS, false);
		properties.validateString(CATALOG_ODPS_ACCESS_ID, false, 1);
		properties.validateString(CATALOG_ODPS_ACCESS_KEY, false, 1);
		properties.validateString(CATALOG_ODPS_PROJECT, false, 1);
		properties.validateString(CATALOG_ODPS_ENDPOINT, false, 1);
		properties.validateString(CATALOG_ODPS_RUNNING_PROJECT, true, 1);
	}
}
