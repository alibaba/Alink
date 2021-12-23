package com.alibaba.alink.common.dl.plugin;

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.common.io.plugin.ClassLoaderContainer;
import com.alibaba.alink.common.io.plugin.ClassLoaderFactory;
import com.alibaba.alink.common.io.plugin.PluginDistributeCache;
import com.alibaba.alink.common.io.plugin.OsType;
import com.alibaba.alink.common.io.plugin.OsUtils;
import com.alibaba.alink.common.io.plugin.PluginDescriptor;
import com.alibaba.alink.common.io.plugin.RegisterKey;
import com.alibaba.alink.common.io.plugin.TemporaryClassLoaderContext;

import java.util.Iterator;
import java.util.ServiceLoader;
import java.util.function.Function;
import java.util.function.Predicate;

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

	private static class DLPredictorServiceFilter implements Predicate <DLPredictorService> {
		private final RegisterKey registerKey;

		public DLPredictorServiceFilter(RegisterKey registerKey) {
			this.registerKey = registerKey;
		}

		@Override
		public boolean test(DLPredictorService service) {
			return true;
		}
	}

	private static class DLPredictorVersionGetter
		implements Function <Tuple2 <DLPredictorService, PluginDescriptor>, String> {
		@Override
		public String apply(Tuple2 <DLPredictorService, PluginDescriptor> service) {
			return service.f1.getVersion();
		}
	}
}
