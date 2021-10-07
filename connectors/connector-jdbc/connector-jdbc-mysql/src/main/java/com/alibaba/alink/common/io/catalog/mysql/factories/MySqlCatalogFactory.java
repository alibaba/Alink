package com.alibaba.alink.common.io.catalog.mysql.factories;

import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.descriptors.CatalogDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.CatalogFactory;

import com.alibaba.alink.common.io.catalog.mysql.MySqlCatalog;
import com.alibaba.alink.common.io.catalog.mysql.descriptors.MySqlCatalogValidator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class MySqlCatalogFactory implements CatalogFactory {

	@Override
	public Map <String, String> requiredContext() {
		Map <String, String> context = new HashMap <>();
		context.put(CatalogDescriptorValidator.CATALOG_TYPE, MySqlCatalogValidator.CATALOG_TYPE_VALUE_MYSQL);
		context.put(CatalogDescriptorValidator.CATALOG_PROPERTY_VERSION, "1");
		return context;
	}

	@Override
	public List <String> supportedProperties() {
		List <String> properties = new ArrayList <>();

		properties.add(CatalogDescriptorValidator.CATALOG_DEFAULT_DATABASE);

		properties.add(MySqlCatalogValidator.CATALOG_MYSQL_URL);
		properties.add(MySqlCatalogValidator.CATALOG_MYSQL_PORT);
		properties.add(MySqlCatalogValidator.CATALOG_MYSQL_USERNAME);
		properties.add(MySqlCatalogValidator.CATALOG_MYSQL_PASSWORD);

		return properties;
	}

	@Override
	public Catalog createCatalog(String name, Map <String, String> properties) {
		final DescriptorProperties descriptorProperties = getValidatedProperties(properties);

		final String defaultDatabase =
			descriptorProperties.getOptionalString(CatalogDescriptorValidator.CATALOG_DEFAULT_DATABASE)
				.orElse(null);

		final Optional <String> mysqlUrl = descriptorProperties.getOptionalString(
			MySqlCatalogValidator.CATALOG_MYSQL_URL);

		final Optional <String> port = descriptorProperties.getOptionalString(
			MySqlCatalogValidator.CATALOG_MYSQL_PORT);

		final Optional <String> userName = descriptorProperties.getOptionalString(
			MySqlCatalogValidator.CATALOG_MYSQL_USERNAME);

		final Optional <String> password = descriptorProperties.getOptionalString(
			MySqlCatalogValidator.CATALOG_MYSQL_PASSWORD);

		return new MySqlCatalog(
			name, defaultDatabase,
			mysqlUrl.orElse(null), port.orElse(null),
			userName.orElse(null), password.orElse(null)
		);
	}

	private static DescriptorProperties getValidatedProperties(Map <String, String> properties) {

		final DescriptorProperties descriptorProperties = new DescriptorProperties(false);
		descriptorProperties.putProperties(properties);

		new MySqlCatalogValidator().validate(descriptorProperties);

		return descriptorProperties;
	}
}
