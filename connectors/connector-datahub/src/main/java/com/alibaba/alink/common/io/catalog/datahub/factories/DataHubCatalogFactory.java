package com.alibaba.alink.common.io.catalog.datahub.factories;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;

import com.alibaba.alink.common.io.catalog.datahub.DataHubCatalog;

import java.util.HashSet;
import java.util.Set;

public class DataHubCatalogFactory implements CatalogFactory {

	@Override
	public Catalog createCatalog(Context context) {

		final FactoryUtil.CatalogFactoryHelper helper =
			FactoryUtil.createCatalogFactoryHelper(this, context);
		helper.validate();

		return new DataHubCatalog(
			helper.getOptions().get(DataHubCatalogFactoryOptions.ACCESS_ID),
			helper.getOptions().get(DataHubCatalogFactoryOptions.ACCESS_KEY),
			helper.getOptions().get(DataHubCatalogFactoryOptions.PROJECT),
			helper.getOptions().get(DataHubCatalogFactoryOptions.ENDPOINT)
		);
	}

	@Override
	public String factoryIdentifier() {
		return DataHubCatalogFactoryOptions.IDENTIFIER;
	}

	@Override
	public Set <ConfigOption <?>> requiredOptions() {
		final Set <ConfigOption <?>> options = new HashSet <>();
		options.add(DataHubCatalogFactoryOptions.ACCESS_ID);
		options.add(DataHubCatalogFactoryOptions.ACCESS_KEY);
		options.add(DataHubCatalogFactoryOptions.PROJECT);
		options.add(DataHubCatalogFactoryOptions.ENDPOINT);
		return options;
	}

	@Override
	public Set <ConfigOption <?>> optionalOptions() {
		final Set <ConfigOption <?>> options = new HashSet <>();
		options.add(DataHubCatalogFactoryOptions.DEFAULT_DATABASE);
		return options;
	}
}
