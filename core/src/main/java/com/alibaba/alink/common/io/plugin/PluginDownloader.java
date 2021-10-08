package com.alibaba.alink.common.io.plugin;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.shaded.guava18.com.google.common.io.Files;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.dl.utils.ZipFileUtil;
import com.alibaba.alink.common.pyrunner.TarFileUtil;
import com.alibaba.alink.common.utils.JsonConverter;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PluginDownloader {

	private final static String MAIN_URL = "https://alink-release.oss-cn-beijing.aliyuncs.com/deps-files";

	private Map <String, PluginDownloaderConfig> jarsPluginConfigs;
	private boolean isJarsPluginConfigLoaded = false;

	private Map <String, PluginDownloaderConfig> resourcePluginConfigs;
	private boolean isResourcePluginConfigLoaded = false;

	public PluginDownloader() {}

	public List <String> listAvailablePlugins() throws IOException {
		loadPluginConfig();

		List <String> all = new ArrayList <>(jarsPluginConfigs.keySet());
		all.addAll(resourcePluginConfigs.keySet());

		return Collections.unmodifiableList(all);
	}

	public List <String> listAvailablePluginVersions(String pluginName) throws IOException {
		loadPluginConfig();

		if (jarsPluginConfigs.containsKey(pluginName)) {
			Map <String, List <String>> allVersions = jarsPluginConfigs.get(pluginName).versions;
			return new ArrayList <>(allVersions.keySet());
		} else if (resourcePluginConfigs.containsKey(pluginName)) {
			Map <String, List <String>> allVersions = resourcePluginConfigs.get(pluginName).versions;
			return new ArrayList <>(allVersions.keySet());
		} else {
			throw new IllegalArgumentException("plugin [" + pluginName + "] not found!");
		}
	}

	public void downloadPlugin(String pluginName, String pluginVersion) throws IOException {
		loadPluginConfig();

		if (jarsPluginConfigs.containsKey(pluginName)) {
			List <String> jars = getListOfJars(pluginName, pluginVersion);
			String remotePath = getRemoteFlinkRoot() + "/" + pluginName + "-" + pluginVersion + "/";
			String localPath = getLocalFlinkRoot() + "/" + pluginName + "-" + pluginVersion + "/";
			for (String jar : jars) {
				download(remotePath + jar, localPath + jar);
			}
		} else if (resourcePluginConfigs.containsKey(pluginName)) {
			List <String> resources = getListOfResource(pluginName, pluginVersion);
			String remotePath = getRemoteResourceRoot() + "/" + pluginName + "-" + pluginVersion + "/";
			String localPath = getLocalResourceRoot() + "/" + pluginName + "-" + pluginVersion + "/";

			for (String resource : resources) {
				download(remotePath + resource, localPath + resource);
			}
		} else {
			throw new IllegalArgumentException("plugin [" + pluginName + "] not found!");
		}
	}

	public void downloadPlugin(String pluginName) throws IOException {
		loadPluginConfig();

		downloadPlugin(pluginName, getDefaultPluginVersion(pluginName));
	}

	public void downloadAll() throws IOException {
		List <String> plugins = this.listAvailablePlugins();
		for (String plugin : plugins) {
			downloadPlugin(plugin);
		}
	}

	/**
	 * Scans all existing plugins and re-download them more carefully.
	 */
	public void upgrade() throws IOException {
		loadPluginConfig();

		File root = new File(getLocalFlinkRoot());
		File[] pluginFiles = root.listFiles();

		if (pluginFiles != null) {
			upgradePlugin(pluginFiles);
		}

		root = new File(getLocalResourceRoot());
		pluginFiles = root.listFiles();

		if (pluginFiles != null) {
			upgradePlugin(pluginFiles);
		}
	}

	public void upgradePlugin(File[] pluginFiles) throws IOException {
		// backup and download all plugins
		for (File pluginFile : pluginFiles) {
			String pluginDirName = pluginFile.getName();
			int dashIndex = pluginDirName.indexOf("-");
			if (dashIndex == -1) {
				// file is "config.json"
				continue;
			}
			String pluginName = pluginDirName.substring(0, dashIndex);
			String pluginVersion = pluginDirName.substring(dashIndex + 1);
			backupPlugin(pluginFile);
			downloadPlugin(pluginName, pluginVersion);
		}

		// delete all backups
		for (File pluginFile : pluginFiles) {
			String pluginDirName = pluginFile.getName();
			int dashIndex = pluginDirName.indexOf("-");
			if (dashIndex == -1) {
				// file is "config.json"
				continue;
			}
			deletePlugin(new File(pluginFile.getAbsolutePath() + ".old"));
		}
	}

	private void loadPluginConfig() throws IOException {
		if (!isJarsPluginConfigLoaded) {
			loadJarsPluginConfig();
		}

		if (!isResourcePluginConfigLoaded) {
			loadResourcePluginConfig();
		}
	}

	private String getLocalFlinkRoot() {
		String pluginDir = AlinkGlobalConfiguration.getPluginDir();
		if (null == pluginDir) {
			throw new IllegalArgumentException("Alink plugin directory not set!");
		}
		return pluginDir + "/" + AlinkGlobalConfiguration.getFlinkVersion();
	}

	private String getRemoteFlinkRoot() {
		return MAIN_URL + "/" + AlinkGlobalConfiguration.getFlinkVersion();
	}

	private String getLocalResourceRoot() {
		String pluginDir = AlinkGlobalConfiguration.getPluginDir();
		if (null == pluginDir) {
			throw new IllegalArgumentException("Alink plugin directory not set!");
		}
		return pluginDir + "/" + ResourcesPluginDirectory.RESOURCE_FOLDER;
	}

	private String getRemoteResourceRoot() {
		return MAIN_URL + "/" + ResourcesPluginDirectory.RESOURCE_FOLDER;
	}

	/**
	 * download config.json from remote and load it (force).
	 */
	public void loadJarsPluginConfig() throws IOException {
		jarsPluginConfigs = JsonConverter.fromJson(
			loadPluginConfig("config.json", getRemoteFlinkRoot(), getLocalFlinkRoot()),
			new TypeReference <Map <String, PluginDownloaderConfig>>() {}.getType()
		);
		isJarsPluginConfigLoaded = true;
	}

	public void loadResourcePluginConfig() throws IOException {
		resourcePluginConfigs = JsonConverter.fromJson(
			loadPluginConfig("config.json", getRemoteResourceRoot(), getLocalResourceRoot()),
			new TypeReference <Map <String, PluginDownloaderConfig>>() {}.getType()
		);
		isResourcePluginConfigLoaded = true;
	}

	@VisibleForTesting
	void loadConfigFromString(String jsonString) {
		if (!isJarsPluginConfigLoaded) {
			jarsPluginConfigs = JsonConverter.fromJson(jsonString,
				new TypeReference <Map <String, PluginDownloaderConfig>>() {}.getType());
		}
		isJarsPluginConfigLoaded = true;
	}

	@VisibleForTesting
	void loadResourceConfigFromString(String jsonString) {
		if (!isResourcePluginConfigLoaded) {
			resourcePluginConfigs = JsonConverter.fromJson(jsonString,
				new TypeReference <Map <String, PluginDownloaderConfig>>() {}.getType());
		}

		isResourcePluginConfigLoaded = true;
	}

	private String loadPluginConfig(String configFileName, String remotePath, String localPath) throws IOException {
		remotePath = String.format("%s/%s", remotePath, configFileName);
		localPath = String.format("%s/%s", localPath, configFileName);

		File configFile = new File(localPath);
		if (configFile.exists()) {
			configFile.delete();
		}
		download(remotePath, localPath);
		return Files.toString(configFile, StandardCharsets.UTF_8);
	}

	private List <String> getListOfResource(String pluginName, String pluginVersion) throws IOException {
		if (!isResourcePluginConfigLoaded) {
			loadResourcePluginConfig();
		}

		if (resourcePluginConfigs.containsKey(pluginName)) {
			Map <String, List <String>> versions = resourcePluginConfigs.get(pluginName).versions;
			if (versions.containsKey(pluginVersion)) {
				return versions.get(pluginVersion);
			} else {
				throw new IllegalArgumentException(
					"plugin [" + pluginName + "], version [" + pluginVersion + "] not found!");
			}
		} else {
			throw new IllegalArgumentException("plugin [" + pluginName + "] not found!");
		}
	}

	private List <String> getListOfJars(String pluginName, String pluginVersion) throws IOException {
		if (!isJarsPluginConfigLoaded) {
			loadJarsPluginConfig();
		}
		if (jarsPluginConfigs.containsKey(pluginName)) {
			Map <String, List <String>> versions = jarsPluginConfigs.get(pluginName).versions;
			if (versions.containsKey(pluginVersion)) {
				return versions.get(pluginVersion);
			} else {
				throw new IllegalArgumentException(
					"plugin [" + pluginName + "], version [" + pluginVersion + "] not found!");
			}
		} else {
			throw new IllegalArgumentException("plugin [" + pluginName + "] not found!");
		}
	}

	private String getDefaultPluginVersion(String pluginName) throws IOException {
		loadPluginConfig();

		if (jarsPluginConfigs.containsKey(pluginName)) {
			return jarsPluginConfigs.get(pluginName).defaultVersion;
		} else if (resourcePluginConfigs.containsKey(pluginName)) {
			return resourcePluginConfigs.get(pluginName).defaultVersion;
		} else {
			throw new IllegalArgumentException("plugin [" + pluginName + "] not found!");
		}
	}

	private void download(String url, String localPath) throws IOException {
		URL httpurl = new URL(url);
		File f = new File(localPath);
		if (!f.exists()) {
			if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
				System.out.println(String.format("Downloading %s to %s", httpurl, localPath));
			}
			FileUtils.copyURLToFile(httpurl, f);

			if (localPath.endsWith(".tar")) {
				TarFileUtil.unTar(f, f.getParentFile(), false);
			} else if (localPath.endsWith(".tar.gz")) {
				TarFileUtil.unTar(f, f.getParentFile(), true);
			} else if (localPath.endsWith(".zip")) {
				ZipFileUtil.unzipFileIntoDirectory(f, f.getParentFile());
			}
		} else {
			if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
				System.out.println(String.format("%s already downloaded to %s", httpurl, localPath));
			}
		}
	}

	private void deletePlugin(File pluginDir) throws IOException {
		if (pluginDir.exists()) {
			FileUtils.deleteDirectory(pluginDir);
		}
	}

	private void backupPlugin(File pluginDir) throws IOException {
		pluginDir.renameTo(new File(pluginDir.getAbsolutePath() + ".old"));
	}
}
