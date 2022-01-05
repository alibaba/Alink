package com.alibaba.alink.common.io.plugin;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.exceptions.DistributePluginException;
import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.util.Map;

public class PluginDistributeCache extends DistributeCache {
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
		String name = context.get("name");
		String version = context.get("version");
		String pluginDir = context.get(PluginConfig.ENV_ALINK_PLUGINS_DIR);

		PluginDownloader downloader = new PluginDownloader(pluginDir);

		if (downloader.checkPluginExistRoughly(name, version)) {
			return;
		}

		if (!Boolean.parseBoolean(context.get("autoPluginDownload"))) {
			throw new DistributePluginException(
				String.format(
					"Distribute [%s-%s] error because autoPluginDownload is false and plugin do not exist.",
					name, version
				)
			);
		}

		downloader.downloadPluginSafely(name, version);
	}

	public static DistributeCache createDistributeCache(String pluginName, String pluginVersion) {
		return new PluginDistributeCacheGenerator().generate(
			ImmutableMap. <String, String>builder()
				.put("name", pluginName)
				.put("version", pluginVersion)
				.put(PluginConfig.ENV_ALINK_PLUGINS_DIR, AlinkGlobalConfiguration.getPluginDir())
				.put("autoPluginDownload", Boolean.toString(AlinkGlobalConfiguration.getAutoPluginDownload()))
				.build()
		);
	}

}
