package com.alibaba.alink.common.io.catalog.plugin;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.descriptors.CatalogDescriptorValidator;
import org.apache.flink.table.factories.TableFactory;

import com.alibaba.alink.common.io.plugin.ClassLoaderContainer;
import com.alibaba.alink.common.io.plugin.ClassLoaderFactory;
import com.alibaba.alink.common.io.plugin.PluginDescriptor;
import com.alibaba.alink.common.io.plugin.PluginDistributeCache;
import com.alibaba.alink.common.io.plugin.RegisterKey;

import java.io.Serializable;
import java.util.function.Function;
import java.util.function.Predicate;

public class OdpsClassLoaderFactory extends ClassLoaderFactory implements Serializable {

	public OdpsClassLoaderFactory(String version) {
		super(new RegisterKey("odps", version), PluginDistributeCache.createDistributeCache("odps", version));
	}

	@Override
	public ClassLoader create() {
		return ClassLoaderContainer
			.getInstance()
			.create(
				registerKey, distributeCache, TableFactory.class,
				new OdpsCatalogServiceFilter(registerKey),
				new OdpsCatalogVersionGetter()
			);
	}

	private static class OdpsCatalogServiceFilter implements Predicate <TableFactory> {
		private final RegisterKey registerKey;

		public OdpsCatalogServiceFilter(RegisterKey registerKey) {
			this.registerKey = registerKey;
		}

		@Override
		public boolean test(TableFactory factory) {
			String catalogType = factory.requiredContext().get(CatalogDescriptorValidator.CATALOG_TYPE);

			return catalogType != null
				&& catalogType.equalsIgnoreCase(registerKey.getName());
		}
	}

	private static class OdpsCatalogVersionGetter implements
		Function <Tuple2 <TableFactory, PluginDescriptor>, String> {

		@Override
		public String apply(Tuple2 <TableFactory, PluginDescriptor> factory) {
			return factory.f1.getVersion();
		}
	}
}
