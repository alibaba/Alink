package com.alibaba.alink.common.io.plugin;

import java.util.Collections;
import java.util.Iterator;

public class ResourcesPluginManager {

	private final ResourcesPluginDirectory pluginRootFolder;

	public ResourcesPluginManager(ResourcesPluginDirectory pluginRootFolder) {
		this.pluginRootFolder = pluginRootFolder;
	}

	public Iterator <ResourcesPluginDescriptor> iterator(String pluginName, String pluginVersion) {
		ResourcesPluginDescriptor descriptor
			= pluginRootFolder.createPluginDescriptorForSubDirectory(pluginName, pluginVersion);
		return null != descriptor
			? Collections.singletonList(descriptor).iterator()
			: Collections.emptyIterator();
	}
}
