package com.alibaba.alink.common.io.redis;

import com.alibaba.alink.common.io.plugin.ClassLoaderContainer;
import com.alibaba.alink.common.io.plugin.ClassLoaderFactory;
import com.alibaba.alink.common.io.plugin.PluginDistributeCache;
import com.alibaba.alink.common.io.plugin.RegisterKey;
import com.alibaba.alink.common.io.plugin.TemporaryClassLoaderContext;

import java.util.Iterator;
import java.util.ServiceLoader;

public class RedisClassLoaderFactory extends ClassLoaderFactory {
	private final static String REDIS_NAME = "redis";

	public RedisClassLoaderFactory(String version) {
		super(new RegisterKey(REDIS_NAME, version), PluginDistributeCache.createDistributeCache(REDIS_NAME, version));
	}

	public static RedisFactory create(RedisClassLoaderFactory factory) {
		ClassLoader classLoader = factory.create();

		try (TemporaryClassLoaderContext context = TemporaryClassLoaderContext.of(classLoader)) {
			Iterator <RedisFactory> iter = ServiceLoader
				.load(RedisFactory.class, classLoader)
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
			RedisFactory.class,
			redisFactory -> true,
			descriptor -> registerKey.getVersion()
		);
	}
}
