package com.alibaba.alink.common.io.hbase;

import com.alibaba.alink.common.exceptions.AkPluginErrorException;
import com.alibaba.alink.common.io.plugin.ClassLoaderContainer;
import com.alibaba.alink.common.io.plugin.ClassLoaderFactory;
import com.alibaba.alink.common.io.plugin.PluginDistributeCache;
import com.alibaba.alink.common.io.plugin.RegisterKey;
import com.alibaba.alink.common.io.plugin.TemporaryClassLoaderContext;

import java.util.Iterator;
import java.util.ServiceLoader;

public class HBaseClassLoaderFactory extends ClassLoaderFactory {
	private final static String HBASE_NAME = "hbase";

	public HBaseClassLoaderFactory(String version) {
		super(new RegisterKey(HBASE_NAME, version), PluginDistributeCache.createDistributeCache(HBASE_NAME, version));
	}

	public static HBaseFactory create(HBaseClassLoaderFactory factory) {
		ClassLoader classLoader = factory.create();

		try (TemporaryClassLoaderContext context = TemporaryClassLoaderContext.of(classLoader)) {
			Iterator <HBaseFactory> iter = ServiceLoader
				.load(HBaseFactory.class, classLoader)
				.iterator();

			if (iter.hasNext()) {
				return iter.next();
			} else {
				throw new AkPluginErrorException("Could not find the class factory in classloader.");
			}
		}
	}

	@Override
	public ClassLoader create() {
		return ClassLoaderContainer.getInstance().create(
			registerKey,
			distributeCache,
			HBaseFactory.class,
			hBaseFactory -> true,
			descriptor -> registerKey.getVersion()
		);
	}
}
