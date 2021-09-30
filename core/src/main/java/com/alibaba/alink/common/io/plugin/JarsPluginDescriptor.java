package com.alibaba.alink.common.io.plugin;

import java.net.URL;
import java.util.Arrays;

public final class JarsPluginDescriptor extends PluginDescriptor {

	/**
	 * URLs to the plugin resources code. Usually this contains URLs of the jars that will be loaded for the plugin.
	 */
	private final URL[] pluginResourceURLs;

	private final String[] allowedFlinkPackages;

	public JarsPluginDescriptor(String pluginId, URL[] pluginResourceURLs, String[] allowedFlinkPackages,
								String version) {
		super(pluginId, version);

		this.pluginResourceURLs = pluginResourceURLs;
		this.allowedFlinkPackages = allowedFlinkPackages;
	}

	public URL[] getPluginResourceURLs() {
		return pluginResourceURLs;
	}

	public String[] getAllowedFlinkPackages() {
		return allowedFlinkPackages;
	}

	@Override
	public String toString() {
		return "JarsPluginDescriptor{" +
			"pluginId='" + getPluginId() + '\'' +
			", pluginResourceURLs=" + Arrays.toString(pluginResourceURLs) +
			", loaderExcludePatterns=" + Arrays.toString(allowedFlinkPackages) +
			", version=" + getVersion() +
			'}';
	}
}
