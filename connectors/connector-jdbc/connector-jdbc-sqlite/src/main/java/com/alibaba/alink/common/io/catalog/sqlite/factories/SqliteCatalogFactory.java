package com.alibaba.alink.common.io.catalog.sqlite.factories;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;

import com.alibaba.alink.common.io.catalog.sqlite.SqliteCatalog;

import java.util.HashSet;
import java.util.Set;

public class SqliteCatalogFactory implements CatalogFactory {

	@Override
	public Catalog createCatalog(Context context) {
		final FactoryUtil.CatalogFactoryHelper helper =
			FactoryUtil.createCatalogFactoryHelper(this, context);
		helper.validate();

		return new SqliteCatalog(
			context.getName(),
			helper.getOptions().get(SqliteCatalogFactoryOptions.DEFAULT_DATABASE),
			helper.getOptions().get(SqliteCatalogFactoryOptions.URLS).split(","),
			helper.getOptions().get(SqliteCatalogFactoryOptions.USERNAME),
			helper.getOptions().get(SqliteCatalogFactoryOptions.PASSWORD)
		);
	}

	@Override
	public String factoryIdentifier() {
		return SqliteCatalogFactoryOptions.IDENTIFIER;
	}

	@Override
	public Set <ConfigOption <?>> requiredOptions() {
		final Set <ConfigOption <?>> options = new HashSet <>();
		options.add(SqliteCatalogFactoryOptions.URLS);
		return options;
	}

	@Override
	public Set <ConfigOption <?>> optionalOptions() {
		final Set <ConfigOption <?>> options = new HashSet <>();
		options.add(SqliteCatalogFactoryOptions.DEFAULT_DATABASE);
		options.add(SqliteCatalogFactoryOptions.USERNAME);
		options.add(SqliteCatalogFactoryOptions.PASSWORD);
		return options;
	}
}
