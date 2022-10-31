package com.alibaba.alink.common.io.xls;

import com.alibaba.alink.common.exceptions.AkPluginErrorException;
import com.alibaba.alink.common.io.plugin.ClassLoaderContainer;
import com.alibaba.alink.common.io.plugin.ClassLoaderFactory;
import com.alibaba.alink.common.io.plugin.PluginDistributeCache;
import com.alibaba.alink.common.io.plugin.RegisterKey;
import com.alibaba.alink.common.io.plugin.TemporaryClassLoaderContext;

import java.util.Iterator;
import java.util.ServiceLoader;

public class XlsReaderClassLoader extends ClassLoaderFactory {
	private final static String XLS_NAME = "xls";

	public XlsReaderClassLoader(String version) {
		super(new RegisterKey(XLS_NAME, version), PluginDistributeCache.createDistributeCache(XLS_NAME, version));
	}

	public static XlsReader create(XlsReaderClassLoader factory) {
		ClassLoader classLoader = factory.create();
		
		try (TemporaryClassLoaderContext context = TemporaryClassLoaderContext.of(classLoader)) {
			Iterator <XlsReader> iter = ServiceLoader
				.load(XlsReader.class, classLoader)
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
			XlsReader.class,
			xlsReader -> true,
			descriptor -> registerKey.getVersion()
		);
	}
}
