package com.alibaba.alink.common.io.catalog.derby.factories;

import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.descriptors.CatalogDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.CatalogFactory;

import com.alibaba.alink.common.io.catalog.derby.DerbyCatalog;
import com.alibaba.alink.common.io.catalog.derby.descriptors.DerbyCatalogValidator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class DerbyCatalogFactory implements CatalogFactory {

	@Override
	public Map <String, String> requiredContext() {
		Map <String, String> context = new HashMap <>();
		context.put(CatalogDescriptorValidator.CATALOG_TYPE, DerbyCatalogValidator.CATALOG_TYPE_VALUE_DERBY);
		context.put(CatalogDescriptorValidator.CATALOG_PROPERTY_VERSION, "1");
		return context;
	}

	@Override
	public List <String> supportedProperties() {
		List <String> properties = new ArrayList <>();

		properties.add(CatalogDescriptorValidator.CATALOG_DEFAULT_DATABASE);

		properties.add(DerbyCatalogValidator.CATALOG_DERBY_PATH);

		properties.add(DerbyCatalogValidator.CATALOG_DERBY_USERNAME);

		properties.add(DerbyCatalogValidator.CATALOG_DERBY_PASSWORD);

		return properties;
	}

	@Override
	public Catalog createCatalog(String name, Map <String, String> properties) {
		final DescriptorProperties descriptorProperties = getValidatedProperties(properties);

		final String defaultDatabase =
			descriptorProperties.getOptionalString(CatalogDescriptorValidator.CATALOG_DEFAULT_DATABASE)
				.orElse(null);

		final Optional <String> derbyPath = descriptorProperties.getOptionalString(
			DerbyCatalogValidator.CATALOG_DERBY_PATH);

		final Optional <String> userName = descriptorProperties.getOptionalString(
			DerbyCatalogValidator.CATALOG_DERBY_USERNAME);

		final Optional <String> password = descriptorProperties.getOptionalString(
			DerbyCatalogValidator.CATALOG_DERBY_PASSWORD);

		return new DerbyCatalog(name, defaultDatabase, derbyPath.orElse(null), userName.orElse(null),
			password.orElse(null));
	}

	private static DescriptorProperties getValidatedProperties(Map <String, String> properties) {
		final DescriptorProperties descriptorProperties = new DescriptorProperties(false);
		descriptorProperties.putProperties(properties);

		new DerbyCatalogValidator().validate(descriptorProperties);

		return descriptorProperties;
	}
}
