package com.alibaba.alink.common.dl.plugin;

import com.alibaba.alink.common.exceptions.AkPluginErrorException;
import com.alibaba.alink.common.io.plugin.ClassLoaderContainer;
import com.alibaba.alink.common.io.plugin.ClassLoaderFactory;
import com.alibaba.alink.common.io.plugin.OsType;
import com.alibaba.alink.common.io.plugin.OsUtils;
import com.alibaba.alink.common.io.plugin.PluginDistributeCache;
import com.alibaba.alink.common.io.plugin.RegisterKey;
import com.alibaba.alink.common.io.plugin.TemporaryClassLoaderContext;

import java.util.Iterator;
import java.util.ServiceLoader;

public class TFPredictorClassLoaderFactory extends ClassLoaderFactory {

	private static final String BASENAME = "tf_predictor_";

	private static final String VERSION = "0.02";

	public TFPredictorClassLoaderFactory() {
		this(OsUtils.getSystemType(), VERSION);
	}

	protected TFPredictorClassLoaderFactory(OsType systemType, String version) {
		super(new RegisterKey(BASENAME + systemType.toString().toLowerCase(), version),
			PluginDistributeCache.createDistributeCache(BASENAME + systemType.toString().toLowerCase(), version));
	}

	public static RegisterKey getRegisterKey() {
		return new RegisterKey(BASENAME + OsUtils.getSystemType().toString().toLowerCase(), VERSION);
	}

	public static DLPredictorService create(TFPredictorClassLoaderFactory factory) {
		ClassLoader classLoader = factory.create();
		try (TemporaryClassLoaderContext context = TemporaryClassLoaderContext.of(classLoader)) {
			Iterator <DLPredictorService> iter = ServiceLoader
				.load(DLPredictorService.class, classLoader)
				.iterator();
			if (iter.hasNext()) {
				return iter.next();
			} else {
				throw new AkPluginErrorException("Could not find the service in the classloader.");
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
