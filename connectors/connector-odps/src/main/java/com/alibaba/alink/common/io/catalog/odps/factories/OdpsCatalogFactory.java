package com.alibaba.alink.common.io.catalog.odps.factories;

import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.descriptors.CatalogDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.CatalogFactory;

import com.alibaba.alink.common.io.catalog.DerbyCatalog;
import com.alibaba.alink.common.io.catalog.odps.OdpsCatalog;
import com.alibaba.alink.common.io.catalog.odps.descriptors.OdpsCatalogValidator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class OdpsCatalogFactory implements CatalogFactory {

	@Override
	public Catalog createCatalog(String name, Map <String, String> properties) {

		final DescriptorProperties descriptorProperties = getValidatedProperties(properties);

		final String accessId = descriptorProperties.getString(
			OdpsCatalogValidator.CATALOG_ODPS_ACCESS_ID
		);

		final String accessKey = descriptorProperties.getString(
			OdpsCatalogValidator.CATALOG_ODPS_ACCESS_KEY
		);

		final String project = descriptorProperties.getString(
			OdpsCatalogValidator.CATALOG_ODPS_PROJECT
		);

		final String endPoint = descriptorProperties.getString(
			OdpsCatalogValidator.CATALOG_ODPS_ENDPOINT);

		final Optional <String> runningProject = descriptorProperties.getOptionalString(
			OdpsCatalogValidator.CATALOG_ODPS_RUNNING_PROJECT);

		return new OdpsCatalog(accessId, accessKey, project, endPoint, runningProject.orElse(null));
	}

	@Override
	public Map <String, String> requiredContext() {
		Map <String, String> context = new HashMap <>();
		context.put(CatalogDescriptorValidator.CATALOG_TYPE, OdpsCatalogValidator.CATALOG_TYPE_VALUE_ODPS);
		context.put(CatalogDescriptorValidator.CATALOG_PROPERTY_VERSION, "1");
		return context;
	}

	@Override
	public List <String> supportedProperties() {
		List <String> properties = new ArrayList <>();

		properties.add(OdpsCatalogValidator.CATALOG_ODPS_ACCESS_ID);

		properties.add(OdpsCatalogValidator.CATALOG_ODPS_ACCESS_KEY);

		properties.add(OdpsCatalogValidator.CATALOG_ODPS_PROJECT);

		properties.add(OdpsCatalogValidator.CATALOG_ODPS_ENDPOINT);

		properties.add(OdpsCatalogValidator.CATALOG_ODPS_RUNNING_PROJECT);

		return properties;
	}

	private static DescriptorProperties getValidatedProperties(Map <String, String> properties) {
		final DescriptorProperties descriptorProperties = new DescriptorProperties(false);
		descriptorProperties.putProperties(properties);

		new OdpsCatalogValidator().validate(descriptorProperties);

		return descriptorProperties;
	}
}
