package com.alibaba.alink.common.io.plugin;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.exceptions.PluginNotExistException;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.common.io.filesystem.LocalFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;

public class ResourcePluginFactory implements Serializable {
	private static final Logger LOG = LoggerFactory.getLogger(ResourcePluginFactory.class);

	private final Map <String, String> globalContext;

	public ResourcePluginFactory() {
		globalContext = PluginDistributeCache.defaultGlobalContext();
	}

	public FilePath getResourcePluginPath(RegisterKey registerKey, RegisterKey... candidates) throws IOException {
		LOG.info("Get resource plugin register key: {}, candidates: {}", registerKey, candidates);
		String pluginDir = globalContext.get(PluginConfig.ENV_ALINK_PLUGINS_DIR);
		String pluginUrl = globalContext.get(AlinkGlobalConfiguration.ALINK_PLUGIN_URL);

		PluginDownloader pluginDownloader = new PluginDownloader(pluginUrl, pluginDir);

		if (pluginDownloader.checkPluginExistRoughly(registerKey.getName(), registerKey.getVersion())) {
			LOG.info("Get resource plugin register key: {}", registerKey);
			return new FilePath(
				pluginDownloader.localResourcePluginPath(registerKey.getName(), registerKey.getVersion()),
				new LocalFileSystem()
			);
		}

		for (RegisterKey candidate : candidates) {
			if (pluginDownloader.checkPluginExistRoughly(candidate.getName(), candidate.getVersion())) {
				LOG.info("Get resource plugin register key: {}", candidate);
				return new FilePath(
					pluginDownloader.localResourcePluginPath(candidate.getName(), candidate.getVersion()),
					new LocalFileSystem()
				);
			}
		}

		LOG.info("Start to distribute {}", registerKey);

		DistributeCache distributeCache = PluginDistributeCache
			.createDistributeCache(registerKey.getName(), registerKey.getVersion(), globalContext);

		distributeCache.distributeAsLocalFile();

		ResourcesPluginManager manager = PluginUtils.createResourcesPluginManagerFromRootFolder(
			PluginUtils.readPluginConf(distributeCache.context())
		);

		Iterator <ResourcesPluginDescriptor> iterator =
			manager.iterator(registerKey.getName(), registerKey.getVersion());

		if (iterator.hasNext()) {
			return iterator.next().getRootFolder();
		} else {
			throw new PluginNotExistException(
				String.format(
					"Could not find the appropriate resource plugin. name: %s, version: %s",
					registerKey.getName(), registerKey.getVersion()
				)
			);
		}
	}
}
