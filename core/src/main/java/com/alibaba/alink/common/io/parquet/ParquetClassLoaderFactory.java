package com.alibaba.alink.common.io.parquet;

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.common.io.plugin.ClassLoaderContainer;
import com.alibaba.alink.common.io.plugin.ClassLoaderFactory;
import com.alibaba.alink.common.io.plugin.PluginDistributeCache;
import com.alibaba.alink.common.io.plugin.PluginDescriptor;
import com.alibaba.alink.common.io.plugin.RegisterKey;
import com.alibaba.alink.common.io.plugin.TemporaryClassLoaderContext;

import java.util.Iterator;
import java.util.ServiceLoader;
import java.util.function.Function;
import java.util.function.Predicate;

public class ParquetClassLoaderFactory extends ClassLoaderFactory {
	public final static String PARQUET_NAME = "parquet";

	public ParquetClassLoaderFactory(String version) {
		super(new RegisterKey(PARQUET_NAME, version), PluginDistributeCache.createDistributeCache(PARQUET_NAME, version));
	}

	public static ParquetSourceFactory create(ParquetClassLoaderFactory factory) {
		ClassLoader classLoader = factory.create();

		try (TemporaryClassLoaderContext context = TemporaryClassLoaderContext.of(classLoader)) {
			Iterator<ParquetSourceFactory> iter = ServiceLoader
				.load(ParquetSourceFactory.class, classLoader)
				.iterator();

			if (iter.hasNext()) {
				return iter.next();
			} else {
				throw new RuntimeException("Could not find the class factory in classloader.");
			}
		}
	}

	@Override
	public ClassLoader create() {
		return ClassLoaderContainer.getInstance().create(
			registerKey,
			distributeCache,
			ParquetSourceFactory.class,
			new ParquetServiceFilter(),
			new ParquetVersionGetter()
		);
	}

	private static class ParquetServiceFilter implements Predicate<ParquetSourceFactory> {

		@Override
		public boolean test(ParquetSourceFactory factory) {
			return true;
		}
	}

	private static class ParquetVersionGetter implements
		Function<Tuple2<ParquetSourceFactory, PluginDescriptor>, String> {

		@Override
		public String apply(Tuple2<ParquetSourceFactory, PluginDescriptor> factory) {
			return factory.f1.getVersion();
		}
	}

}

