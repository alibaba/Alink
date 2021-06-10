package com.alibaba.alink.common.io.catalog.derby.factories;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;

import com.alibaba.alink.common.io.catalog.derby.DerbyCatalog;

import java.util.HashSet;
import java.util.Set;

public class DerbyCatalogFactory implements CatalogFactory {

	@Override
	public Catalog createCatalog(Context context) {
		final FactoryUtil.CatalogFactoryHelper helper =
			FactoryUtil.createCatalogFactoryHelper(this, context);
		helper.validate();

		return new DerbyCatalog(
			context.getName(),
			helper.getOptions().get(DerbyCatalogFactoryOptions.DEFAULT_DATABASE),
			helper.getOptions().get(DerbyCatalogFactoryOptions.PATH),
			helper.getOptions().get(DerbyCatalogFactoryOptions.USERNAME),
			helper.getOptions().get(DerbyCatalogFactoryOptions.PASSWORD)
		);
	}

	@Override
	public String factoryIdentifier() {
		return DerbyCatalogFactoryOptions.IDENTIFIER;
	}

	@Override
	public Set <ConfigOption <?>> requiredOptions() {
		final Set <ConfigOption <?>> options = new HashSet <>();
		options.add(DerbyCatalogFactoryOptions.PATH);
		return options;
	}

	@Override
	public Set <ConfigOption <?>> optionalOptions() {
		final Set <ConfigOption <?>> options = new HashSet <>();
		options.add(DerbyCatalogFactoryOptions.DEFAULT_DATABASE);
		options.add(DerbyCatalogFactoryOptions.USERNAME);
		options.add(DerbyCatalogFactoryOptions.PASSWORD);
		return options;
	}
}
