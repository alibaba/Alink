package com.alibaba.alink.common.io.plugin;

import com.alibaba.alink.common.io.filesystem.FilePath;

public final class ResourcesPluginDescriptor extends PluginDescriptor {

	private final FilePath rootFolder;

	public ResourcesPluginDescriptor(String pluginId, String version, FilePath rootFolder) {
		super(pluginId, version);

		this.rootFolder = rootFolder;
	}

	public FilePath getRootFolder() {
		return rootFolder;
	}
}
