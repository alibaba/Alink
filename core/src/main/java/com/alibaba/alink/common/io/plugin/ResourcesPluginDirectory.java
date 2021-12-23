package com.alibaba.alink.common.io.plugin;

import org.apache.flink.core.fs.Path;

import com.alibaba.alink.common.io.filesystem.FilePath;

import java.io.IOException;

public class ResourcesPluginDirectory {

	public static final String RESOURCE_FOLDER = "resources";

	private final FilePath pluginsRootDir;

	public ResourcesPluginDirectory(FilePath pluginsRootDir) {
		this.pluginsRootDir = pluginsRootDir;
	}

	public ResourcesPluginDescriptor createPluginDescriptorForSubDirectory(
		String pluginName, String pluginVersion) {

		FilePath subDirectory = new FilePath(
			new Path(
				pluginsRootDir.getPath(),
				new Path(RESOURCE_FOLDER, String.format("%s-%s", pluginName, pluginVersion))
			),
			pluginsRootDir.getFileSystem()
		);

		boolean folderExists;

		try {
			folderExists = subDirectory.getFileSystem().exists(subDirectory.getPath());
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}

		if (folderExists) {
			return new ResourcesPluginDescriptor(
				subDirectory.getPath().getName(),
				pluginVersion,
				subDirectory
			);
		} else {
			return null;
		}
	}
}
