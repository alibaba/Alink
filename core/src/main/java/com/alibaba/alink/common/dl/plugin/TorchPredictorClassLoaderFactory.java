package com.alibaba.alink.common.dl.plugin;

import com.alibaba.alink.common.io.plugin.ClassLoaderContainer;
import com.alibaba.alink.common.io.plugin.ClassLoaderFactory;
import com.alibaba.alink.common.io.plugin.PluginDistributeCache;
import com.alibaba.alink.common.io.plugin.RegisterKey;
import com.alibaba.alink.common.io.plugin.TemporaryClassLoaderContext;

import java.util.Iterator;
import java.util.ServiceLoader;

public class TorchPredictorClassLoaderFactory extends ClassLoaderFactory {

	private static final String BASENAME = "torch_predictor";

	public TorchPredictorClassLoaderFactory(String version) {
		super(new RegisterKey(BASENAME, version),
			PluginDistributeCache.createDistributeCache(BASENAME, version));
	}

	public RegisterKey getRegisterKey() {
		return registerKey;
	}

	public static DLPredictorService create(TorchPredictorClassLoaderFactory factory) {
		ClassLoader classLoader = factory.create();
		try (TemporaryClassLoaderContext context = TemporaryClassLoaderContext.of(classLoader)) {
			Iterator <DLPredictorService> iter = ServiceLoader
				.load(DLPredictorService.class, classLoader)
				.iterator();
			if (iter.hasNext()) {
				return iter.next();
			} else {
				throw new RuntimeException("Could not find the service in the classloader.");
			}
		}
	}

	@Override
	public ClassLoader create() {
		return ClassLoaderContainer.getInstance().create(
			registerKey,
			distributeCache,
			DLPredictorService.class,
			new DLPredictorServiceFilter(registerKey),
			new DLPredictorVersionGetter()
		);
	}
}
