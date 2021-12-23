package com.alibaba.alink.common.io.plugin;

import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.ServiceLoader;

@ThreadSafe
public class JarsPluginLoader {

	/**
	 * Classloader which is used to load the plugin classes. We expect this classloader is thread-safe.
	 */
	private final ClassLoader pluginClassLoader;

	public JarsPluginLoader(ClassLoader pluginClassLoader) {
		this.pluginClassLoader = pluginClassLoader;
	}

	public static ClassLoader createPluginClassLoader(JarsPluginDescriptor pluginDescriptor,
													  ClassLoader parentClassLoader,
													  String[] alwaysParentFirstPatterns) {
		/*
		return new ChildFirstClassLoader(
			pluginDescriptor.getPluginResourceURLs(),
			parentClassLoader,
			ArrayUtils.concat(alwaysParentFirstPatterns, pluginDescriptor.getLoaderExcludePatterns()));
		 */

		// Different from flink, when use oss which has been shaded,
		// it will conflict with `flink-hadoop-fs` which has been packed in the flink_dist
		// and we also expect to use flink class loader to make other packages work such as sql.

		return new PluginClassLoader(
			pluginDescriptor.getPluginResourceURLs(),
			parentClassLoader,
			pluginDescriptor.getAllowedFlinkPackages() == null || pluginDescriptor.getAllowedFlinkPackages().length == 0
				? alwaysParentFirstPatterns : pluginDescriptor.getAllowedFlinkPackages()
		);
	}

	public static JarsPluginLoader create(JarsPluginDescriptor pluginDescriptor, ClassLoader parentClassLoader,
										  String[] alwaysParentFirstPatterns) {
		return new JarsPluginLoader(
			createPluginClassLoader(pluginDescriptor, parentClassLoader, alwaysParentFirstPatterns));
	}

	/**
	 * Returns in iterator over all available implementations of the given service interface (SPI) for the plugin.
	 *
	 * @param service the service interface (SPI) for which implementations are requested.
	 * @param <P>     Type of the requested plugin service.
	 * @return An iterator of all implementations of the given service interface that could be loaded from the plugin.
	 */
	public <P> Iterator <P> load(Class <P> service) {
		try (TemporaryClassLoaderContext classLoaderContext = TemporaryClassLoaderContext.of(pluginClassLoader)) {
			return new ContextClassLoaderSettingIterator <>(
				ServiceLoader.load(service, pluginClassLoader).iterator(),
				pluginClassLoader);
		}
	}

	/**
	 * Wrapper for the service iterator. The wrapper will set/unset the context classloader to the plugin classloader
	 * around the point where elements are returned.
	 *
	 * @param <P> type of the iterated plugin element.
	 */
	static class ContextClassLoaderSettingIterator<P> implements Iterator <P> {

		private final Iterator <P> delegate;
		private final ClassLoader pluginClassLoader;

		ContextClassLoaderSettingIterator(Iterator <P> delegate, ClassLoader pluginClassLoader) {
			this.delegate = delegate;
			this.pluginClassLoader = pluginClassLoader;
		}

		@Override
		public boolean hasNext() {
			return delegate.hasNext();
		}

		@Override
		public P next() {
			try (TemporaryClassLoaderContext classLoaderContext = TemporaryClassLoaderContext.of(pluginClassLoader)) {
				return delegate.next();
			}
		}
	}

	/**
	 * Loads all classes from the plugin jar except for explicitly white-listed packages (org.apache.flink, logging).
	 *
	 * <p>No class/resource in the system class loader (everything in lib/) can be seen in the plugin except those
	 * starting with a whitelist prefix.
	 */
	private static final class PluginClassLoader extends URLClassLoader {

		private final ClassLoader flinkClassLoader;

		private final String[] allowedFlinkPackages;

		private final String[] allowedResourcePrefixes;

		PluginClassLoader(URL[] pluginResourceURLs, ClassLoader flinkClassLoader, String[] allowedFlinkPackages) {
			super(pluginResourceURLs, null);
			this.flinkClassLoader = flinkClassLoader;
			this.allowedFlinkPackages = allowedFlinkPackages;
			allowedResourcePrefixes = Arrays.stream(allowedFlinkPackages)
				.map(packageName -> packageName.replace('.', '/'))
				.toArray(String[]::new);
		}

		@Override
		protected Class <?> loadClass(final String name, final boolean resolve) throws ClassNotFoundException {
			synchronized (getClassLoadingLock(name)) {
				final Class <?> loadedClass = findLoadedClass(name);
				if (loadedClass != null) {
					return resolveIfNeeded(resolve, loadedClass);
				}

				if (isAllowedFlinkClass(name)) {
					try {
						return resolveIfNeeded(resolve, flinkClassLoader.loadClass(name));
					} catch (ClassNotFoundException e) {
						// fallback to resolving it in this classloader
						// for cases where the plugin uses org.apache.flink namespace
					}
				}

				return super.loadClass(name, resolve);
			}
		}

		private Class <?> resolveIfNeeded(final boolean resolve, final Class <?> loadedClass) {
			if (resolve) {
				resolveClass(loadedClass);
			}

			return loadedClass;
		}

		@Override
		public URL getResource(final String name) {
			if (isAllowedFlinkResource(name)) {
				return flinkClassLoader.getResource(name);
			}

			return super.getResource(name);
		}

		@Override
		public Enumeration <URL> getResources(final String name) throws IOException {
			// ChildFirstClassLoader merges child and parent resources
			if (isAllowedFlinkResource(name)) {
				return flinkClassLoader.getResources(name);
			}

			return super.getResources(name);
		}

		private boolean isAllowedFlinkClass(final String name) {
			return Arrays.stream(allowedFlinkPackages).anyMatch(name::startsWith);
		}

		private boolean isAllowedFlinkResource(final String name) {
			return Arrays.stream(allowedResourcePrefixes).anyMatch(name::startsWith);
		}
	}
}
