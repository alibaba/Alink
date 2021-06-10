package com.alibaba.alink.common.io.catalog.mysql.factories;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;

import com.alibaba.alink.common.io.catalog.mysql.MySqlCatalog;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class MySqlCatalogFactory implements CatalogFactory {

	@Override
	public Catalog createCatalog(Context context) {
		final FactoryUtil.CatalogFactoryHelper helper =
			FactoryUtil.createCatalogFactoryHelper(this, context);
		helper.validate();

		return new MySqlCatalog(
			context.getName(),
			helper.getOptions().get(MySqlCatalogFactoryOptions.DEFAULT_DATABASE),
			helper.getOptions().get(MySqlCatalogFactoryOptions.URL),
			helper.getOptions().get(MySqlCatalogFactoryOptions.PORT),
			helper.getOptions().get(MySqlCatalogFactoryOptions.USERNAME),
			helper.getOptions().get(MySqlCatalogFactoryOptions.PASSWORD)
		);
	}

	@Override
	public String factoryIdentifier() {
		return MySqlCatalogFactoryOptions.IDENTIFIER;
	}

	@Override
	public Set <ConfigOption <?>> requiredOptions() {
		return Collections.emptySet();
	}

	@Override
	public Set <ConfigOption <?>> optionalOptions() {
		final Set <ConfigOption <?>> options = new HashSet <>();
		options.add(MySqlCatalogFactoryOptions.DEFAULT_DATABASE);
		options.add(MySqlCatalogFactoryOptions.URL);
		options.add(MySqlCatalogFactoryOptions.PORT);
		options.add(MySqlCatalogFactoryOptions.USERNAME);
		options.add(MySqlCatalogFactoryOptions.PASSWORD);
		return options;
	}
}
