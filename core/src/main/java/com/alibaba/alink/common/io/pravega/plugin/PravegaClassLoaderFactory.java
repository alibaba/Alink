package com.alibaba.alink.common.io.pravega.plugin;

import com.alibaba.alink.common.io.plugin.*;
import org.apache.flink.api.java.tuple.Tuple2;

import java.lang.reflect.InvocationTargetException;
import java.util.function.Function;
import java.util.function.Predicate;

public class PravegaClassLoaderFactory extends ClassLoaderFactory {
	public final static String PRAVEGA_NAME = "pravega";

	public PravegaClassLoaderFactory(String version) {
		super(new RegisterKey(PRAVEGA_NAME, version), PluginDistributeCache.createDistributeCache(PRAVEGA_NAME, version));
	}

	public static PravegaSourceSinkFactory create(PravegaClassLoaderFactory factory) {
		try {
			return (PravegaSourceSinkFactory) factory
				.create()
				.loadClass("com.alibaba.alink.common.io.pravega.plugin.PravegaSourceSinkInPluginFactory")
				.getConstructor()
				.newInstance();
		} catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException |
			ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public ClassLoader create() {
		return ClassLoaderContainer.getInstance().create(
			registerKey,
			distributeCache,
			PravegaSourceSinkFactory.class,
			new PravegaServiceFilter(),
			new PravegaVersionGetter()
		);
	}

	private static class PravegaServiceFilter implements Predicate <PravegaSourceSinkFactory> {

		@Override
		public boolean test(PravegaSourceSinkFactory factory) {
			return true;
		}
	}

	private static class PravegaVersionGetter implements
		Function <Tuple2<PravegaSourceSinkFactory, PluginDescriptor>, String> {

		@Override
		public String apply(Tuple2<PravegaSourceSinkFactory, PluginDescriptor> factory) {
			return "0001";
		}
	}

}
