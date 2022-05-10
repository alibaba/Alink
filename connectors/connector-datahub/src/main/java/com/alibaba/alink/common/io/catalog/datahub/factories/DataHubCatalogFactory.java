package com.alibaba.alink.common.io.catalog.datahub.factories;

import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.descriptors.CatalogDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.CatalogFactory;

import com.alibaba.alink.common.io.catalog.datahub.DataHubCatalog;
import com.alibaba.alink.common.io.catalog.datahub.descriptors.DataHubCatalogValidator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataHubCatalogFactory implements CatalogFactory {

	@Override
	public Catalog createCatalog(String name, Map <String, String> properties) {

		final DescriptorProperties descriptorProperties = getValidatedProperties(properties);

		final String accessId = descriptorProperties.getString(
			DataHubCatalogValidator.CATALOG_DATAHUB_ACCESS_ID
		);

		final String accessKey = descriptorProperties.getString(
			DataHubCatalogValidator.CATALOG_DATAHUB_ACCESS_KEY
		);

		final String project = descriptorProperties.getString(
			DataHubCatalogValidator.CATALOG_DATAHUB_PROJECT
		);

		final String endPoint = descriptorProperties.getString(
			DataHubCatalogValidator.CATALOG_DATAHUB_ENDPOINT);

		return new DataHubCatalog(accessId, accessKey, project, endPoint);
	}

	@Override
	public Map <String, String> requiredContext() {
		Map <String, String> context = new HashMap <>();
		context.put(CatalogDescriptorValidator.CATALOG_TYPE, DataHubCatalogValidator.CATALOG_TYPE_VALUE_DATAHUB);
		context.put(CatalogDescriptorValidator.CATALOG_PROPERTY_VERSION, "1");
		return context;
	}

	@Override
	public List <String> supportedProperties() {
		List <String> properties = new ArrayList <>();

		properties.add(DataHubCatalogValidator.CATALOG_DATAHUB_ACCESS_ID);

		properties.add(DataHubCatalogValidator.CATALOG_DATAHUB_ACCESS_KEY);

		properties.add(DataHubCatalogValidator.CATALOG_DATAHUB_PROJECT);

		properties.add(DataHubCatalogValidator.CATALOG_DATAHUB_ENDPOINT);

		return properties;
	}

	private static DescriptorProperties getValidatedProperties(Map <String, String> properties) {
		final DescriptorProperties descriptorProperties = new DescriptorProperties(false);
		descriptorProperties.putProperties(properties);

		new DataHubCatalogValidator().validate(descriptorProperties);

		return descriptorProperties;
	}
}
