package com.alibaba.alink.operator.common.tree.xgboost.plugin;

import com.alibaba.alink.common.exceptions.XGBoostPluginNotExistsException;
import com.alibaba.alink.common.io.plugin.ClassLoaderContainer;
import com.alibaba.alink.common.io.plugin.ClassLoaderFactory;
import com.alibaba.alink.common.io.plugin.PluginDistributeCache;
import com.alibaba.alink.common.io.plugin.RegisterKey;
import com.alibaba.alink.common.io.plugin.TemporaryClassLoaderContext;
import com.alibaba.alink.operator.common.tree.xgboost.XGBoostFactory;

import java.util.Iterator;
import java.util.ServiceLoader;

public class XGBoostClassLoaderFactory extends ClassLoaderFactory {

	private static final String XGBOOST_NAME = "xgboost";

	public XGBoostClassLoaderFactory(String version) {
		super(
			new RegisterKey(XGBOOST_NAME, version),
			PluginDistributeCache.createDistributeCache(XGBOOST_NAME, version)
		);
	}

	public static XGBoostFactory create(XGBoostClassLoaderFactory factory) {
		ClassLoader classLoader = factory.create();
		try (TemporaryClassLoaderContext context = TemporaryClassLoaderContext.of(classLoader)) {
			Iterator <XGBoostFactory> iter = ServiceLoader
				.load(XGBoostFactory.class, classLoader)
				.iterator();
			if (iter.hasNext()) {
				return iter.next();
			} else {
				throw new XGBoostPluginNotExistsException("Could not find the XGBoostFactory in the classloader.");
			}
		}
	}

	@Override
	public ClassLoader create() {
		return ClassLoaderContainer.getInstance().create(
			registerKey,
			distributeCache,
			XGBoostFactory.class,
			xgBoostFactory -> true,
			xgBoostFactoryPluginDescriptorTuple2 -> registerKey.getVersion()
		);
	}
}
