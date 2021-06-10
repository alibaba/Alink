package com.alibaba.alink.common.io.catalog.odps.factories;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;

import com.alibaba.alink.common.io.catalog.odps.OdpsCatalog;

import java.util.HashSet;
import java.util.Set;

public class OdpsCatalogFactory implements CatalogFactory {

	@Override
	public Catalog createCatalog(Context context) {
		final FactoryUtil.CatalogFactoryHelper helper =
			FactoryUtil.createCatalogFactoryHelper(this, context);
		helper.validate();

		return new OdpsCatalog(
			helper.getOptions().get(OdpsCatalogFactoryOptions.ACCESS_ID),
			helper.getOptions().get(OdpsCatalogFactoryOptions.ACCESS_KEY),
			helper.getOptions().get(OdpsCatalogFactoryOptions.PROJECT),
			helper.getOptions().get(OdpsCatalogFactoryOptions.ENDPOINT),
			helper.getOptions().get(OdpsCatalogFactoryOptions.RUNNING_PROJECT)
		);
	}

	@Override
	public String factoryIdentifier() {
		return OdpsCatalogFactoryOptions.IDENTIFIER;
	}

	@Override
	public Set <ConfigOption <?>> requiredOptions() {
		final Set <ConfigOption <?>> options = new HashSet <>();
		options.add(OdpsCatalogFactoryOptions.ACCESS_ID);
		options.add(OdpsCatalogFactoryOptions.ACCESS_KEY);
		options.add(OdpsCatalogFactoryOptions.PROJECT);
		options.add(OdpsCatalogFactoryOptions.ENDPOINT);
		return options;
	}

	@Override
	public Set <ConfigOption <?>> optionalOptions() {
		final Set <ConfigOption <?>> options = new HashSet <>();
		options.add(OdpsCatalogFactoryOptions.DEFAULT_DATABASE);
		options.add(OdpsCatalogFactoryOptions.RUNNING_PROJECT);
		return options;
	}
}
