package com.alibaba.alink.common.io.catalog.plugin;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.factories.Factory;

import com.alibaba.alink.common.io.plugin.ClassLoaderContainer;
import com.alibaba.alink.common.io.plugin.ClassLoaderFactory;
import com.alibaba.alink.common.io.plugin.PluginDescriptor;
import com.alibaba.alink.common.io.plugin.PluginDistributeCache;
import com.alibaba.alink.common.io.plugin.RegisterKey;

import java.io.Serializable;
import java.util.function.Function;
import java.util.function.Predicate;

public class JdbcCatalogClassLoaderFactory extends ClassLoaderFactory implements Serializable {

	private static final long serialVersionUID = -1012808895725991073L;

	public JdbcCatalogClassLoaderFactory(String name, String version) {
		super(new RegisterKey(name, version), PluginDistributeCache.createDistributeCache(name, version));
	}

	@Override
	public ClassLoader create() {
		return ClassLoaderContainer
			.getInstance()
			.create(
				registerKey, distributeCache, Factory.class,
				new JdbcCatalogServiceFilter(registerKey),
				new JdbcCatalogVersionGetter()
			);
	}

	private static class JdbcCatalogServiceFilter implements Predicate <Factory> {
		private final RegisterKey registerKey;

		public JdbcCatalogServiceFilter(RegisterKey registerKey) {
			this.registerKey = registerKey;
		}

		@Override
		public boolean test(Factory factory) {
			String catalogType = factory.factoryIdentifier();

			return catalogType != null
				&& catalogType.equalsIgnoreCase(registerKey.getName())
				&& factory.getClass().getName().contains(registerKey.getName());
		}
	}

	private static class JdbcCatalogVersionGetter implements
		Function <Tuple2 <Factory, PluginDescriptor>, String> {

		@Override
		public String apply(Tuple2 <Factory, PluginDescriptor> factory) {
			return factory.f1.getVersion();
		}
	}
}
