package com.alibaba.alink.common.io.plugin;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.exceptions.DistributePluginException;
import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class PluginDistributeCache extends DistributeCache {

	static final String KEY_NAME = "name";
	static final String KEY_VERSION = "version";
	static final String KEY_AUTO_PLUGIN_DOWNLOAD = "autoPluginDownload";

	private final Map <String, String> context;

	public PluginDistributeCache(Map <String, String> context) {
		this.context = context;
	}

	@Override
	public Map <String, String> context() {
		return context;
	}

	@Override
	public void distributeAsLocalFile() throws IOException {
		String name = context.get(KEY_NAME);
		String version = context.get(KEY_VERSION);
		String pluginDir = context.get(PluginConfig.ENV_ALINK_PLUGINS_DIR);
		String pluginUrl = context.get(AlinkGlobalConfiguration.ALINK_PLUGIN_URL);

		PluginDownloader downloader = new PluginDownloader(pluginUrl, pluginDir);

		if (downloader.checkPluginExistRoughly(name, version)) {
			return;
		}

		if (!Boolean.parseBoolean(context.get(KEY_AUTO_PLUGIN_DOWNLOAD))) {
			throw new DistributePluginException(
				String.format(
					"Distribute [%s-%s] error because autoPluginDownload is false and plugin do not exist.",
					name, version
				)
			);
		}

		downloader.downloadPluginSafely(name, version);
	}

	public static Map <String, String> defaultGlobalContext() {
		Map <String, String> globalContext = new HashMap <>();
		globalContext.put(PluginConfig.ENV_ALINK_PLUGINS_DIR, AlinkGlobalConfiguration.getPluginDir());
		globalContext.put(PluginDistributeCache.KEY_AUTO_PLUGIN_DOWNLOAD,
			Boolean.toString(AlinkGlobalConfiguration.getAutoPluginDownload()));
		globalContext.put(AlinkGlobalConfiguration.ALINK_PLUGIN_URL, AlinkGlobalConfiguration.getPluginUrl());
		return globalContext;
	}

	public static DistributeCache createDistributeCache(String pluginName, String pluginVersion) {
		return createDistributeCache(pluginName, pluginVersion, defaultGlobalContext());
	}

	public static DistributeCache createDistributeCache(String pluginName, String pluginVersion,
														Map <String, String> globalContext) {
		return new PluginDistributeCacheGenerator().generate(
			ImmutableMap. <String, String>builder()
				.putAll(globalContext)
				.put(KEY_NAME, pluginName)
				.put(KEY_VERSION, pluginVersion)
				.build()
		);
	}
}
