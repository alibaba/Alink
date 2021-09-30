package com.alibaba.alink.common.io.plugin;

import com.alibaba.alink.common.io.filesystem.FilePath;

import java.util.Iterator;

public class ResourcePluginFactory {

	public static FilePath getResourcePluginPath(RegisterKey registerKey) {
		ResourcesPluginManager manager = PluginUtils.createResourcesPluginManagerFromRootFolder(
			PluginUtils.readPluginConf(
				ClassLoaderContainer.createPluginContextOnClient()
			)
		);
		Iterator <ResourcesPluginDescriptor> iterator =
			manager.iterator(registerKey.getName(), registerKey.getVersion());
		return iterator.hasNext() ? iterator.next().getRootFolder() : null;
	}
}
