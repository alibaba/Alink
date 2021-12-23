package com.alibaba.alink.common.io.plugin;

import com.alibaba.alink.common.exceptions.PluginNotExistException;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.common.io.filesystem.LocalFileSystem;

import java.io.IOException;
import java.util.Iterator;

public class ResourcePluginFactory {

	public static FilePath getResourcePluginPath(
		RegisterKey registerKey, RegisterKey... candidates) throws IOException {

		PluginDownloader pluginDownloader = new PluginDownloader();

		if (pluginDownloader.checkPluginExistRoughly(registerKey.getName(), registerKey.getVersion())) {
			return new FilePath(
				pluginDownloader.localResourcePluginPath(registerKey.getName(), registerKey.getVersion()),
				new LocalFileSystem()
			);
		}

		for (RegisterKey candidate : candidates) {
			if (pluginDownloader.checkPluginExistRoughly(candidate.getName(), candidate.getVersion())) {
				return new FilePath(
					pluginDownloader.localResourcePluginPath(candidate.getName(), candidate.getVersion()),
					new LocalFileSystem()
				);
			}
		}

		DistributeCache distributeCache = PluginDistributeCache
			.createDistributeCache(registerKey.getName(), registerKey.getVersion());

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
