package com.alibaba.alink.common.io.catalog.sqlite.factories;

import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.descriptors.CatalogDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.CatalogFactory;

import com.alibaba.alink.common.io.catalog.sqlite.SqliteCatalog;
import com.alibaba.alink.common.io.catalog.sqlite.descriptors.SqliteCatalogValidator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class SqliteCatalogFactory implements CatalogFactory {

	@Override
	public Map <String, String> requiredContext() {
		Map <String, String> context = new HashMap <>();
		context.put(
			CatalogDescriptorValidator.CATALOG_TYPE,
			SqliteCatalogValidator.CATALOG_TYPE_VALUE_SQLITE
		);
		context.put(CatalogDescriptorValidator.CATALOG_PROPERTY_VERSION, "1");
		return context;
	}

	@Override
	public List <String> supportedProperties() {
		List <String> properties = new ArrayList <>();

		properties.add(CatalogDescriptorValidator.CATALOG_DEFAULT_DATABASE);

		properties.add(SqliteCatalogValidator.CATALOG_SQLITE_URLS);

		properties.add(SqliteCatalogValidator.CATALOG_SQLITE_USERNAME);

		properties.add(SqliteCatalogValidator.CATALOG_SQLITE_PASSWORD);

		return properties;
	}

	@Override
	public Catalog createCatalog(String name, Map <String, String> properties) {
		final DescriptorProperties descriptorProperties = getValidatedProperties(properties);

		final String defaultDatabase =
			descriptorProperties.getOptionalString(CatalogDescriptorValidator.CATALOG_DEFAULT_DATABASE)
				.orElse(null);

		final Optional <String> sqliteUrls = descriptorProperties.getOptionalString(
			SqliteCatalogValidator.CATALOG_SQLITE_URLS);

		String[] urls = sqliteUrls.orElseThrow(() -> new IllegalArgumentException("")).split(",");

		final Optional <String> userName = descriptorProperties.getOptionalString(
			SqliteCatalogValidator.CATALOG_SQLITE_USERNAME);

		final Optional <String> password = descriptorProperties.getOptionalString(
			SqliteCatalogValidator.CATALOG_SQLITE_PASSWORD);

		return new SqliteCatalog(name, defaultDatabase, urls, userName.orElse(null),
			password.orElse(null));
	}

	private static DescriptorProperties getValidatedProperties(Map <String, String> properties) {
		final DescriptorProperties descriptorProperties = new DescriptorProperties(false);
		descriptorProperties.putProperties(properties);

		new SqliteCatalogValidator().validate(descriptorProperties);

		return descriptorProperties;
	}
}
