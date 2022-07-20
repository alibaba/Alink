package com.alibaba.alink.common.io.parquet;

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.common.exceptions.AkPluginErrorException;
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
		super(new RegisterKey(PARQUET_NAME, version),
			PluginDistributeCache.createDistributeCache(PARQUET_NAME, version));
	}

	public ParquetSourceFactory createParquetSourceFactory() {
		ClassLoader classLoader = ClassLoaderContainer.getInstance().create(registerKey, distributeCache, ParquetSourceFactory.class,
			new ParquetSourceServiceFilter(), new ParquetSourceVersionGetter());

		try (TemporaryClassLoaderContext context = TemporaryClassLoaderContext.of(classLoader)) {
			Iterator <ParquetSourceFactory> iter = ServiceLoader.load(ParquetSourceFactory.class, classLoader)
				.iterator();

			if (iter.hasNext()) {
				return iter.next();
			} else {
				throw new AkPluginErrorException("Could not find ParquetSourceFactory class factory in classloader.");
			}
		}
	}

	public ParquetReaderFactory createParquetReaderFactory() {
		ClassLoader classLoader = ClassLoaderContainer.getInstance().create(registerKey, distributeCache, ParquetReaderFactory.class,
			new ParquetReaderServiceFilter(), new ParquetReaderVersionGetter());

		try (TemporaryClassLoaderContext context = TemporaryClassLoaderContext.of(classLoader)) {
			Iterator<ParquetReaderFactory> iter = ServiceLoader
				.load(ParquetReaderFactory.class, classLoader)
				.iterator();

			if (iter.hasNext()) {
				return iter.next();
			} else {
				throw new AkPluginErrorException("Could not find ParquetReaderFactory class factory in classloader.");
			}
		}
	}
	@Override
	public ClassLoader create() {
		return ClassLoaderContainer.getInstance().create(registerKey, distributeCache, ParquetSourceFactory.class,
			new ParquetSourceServiceFilter(), new ParquetSourceVersionGetter());
	}

	private static class ParquetSourceServiceFilter implements Predicate <ParquetSourceFactory> {
		@Override
		public boolean test(ParquetSourceFactory factory) {
			return true;
		}
	}

	private static class ParquetSourceVersionGetter
		implements Function <Tuple2 <ParquetSourceFactory, PluginDescriptor>, String> {
		@Override
		public String apply(Tuple2 <ParquetSourceFactory, PluginDescriptor> factory) {
			return factory.f1.getVersion();
		}
	}

	private static class ParquetReaderServiceFilter implements Predicate <ParquetReaderFactory> {
		@Override
		public boolean test(ParquetReaderFactory factory) {return true;}
	}

	private static class ParquetReaderVersionGetter
		implements Function <Tuple2 <ParquetReaderFactory, PluginDescriptor>, String> {
		@Override
		public String apply(Tuple2 <ParquetReaderFactory, PluginDescriptor> factory) {
			return factory.f1.getVersion();
		}
	}

}

