package com.alibaba.alink.common.io.plugin;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.guava18.com.google.common.io.Files;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.utils.JsonConverter;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PluginDownloader {

	final String MAIN_URL = "https://alink-release.oss-cn-beijing.aliyuncs.com/deps-files";
	Map <String, PluginDownloaderConfig> configs;
	boolean isConfigLoaded = false;

	public PluginDownloader() { }

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

	/**
	 * download config.json from remote and load it (force).
	 *
	 * @throws IOException
	 */
	public void loadConfig() throws IOException {
		loadConfig("config.json");
	}

	void loadConfigFromString(String jsonString) {
		if (!isConfigLoaded) {
			configs = JsonConverter.fromJson(jsonString,
				new TypeReference <Map <String, PluginDownloaderConfig>>() {}.getType());
		}
		isConfigLoaded = true;
	}

	public void loadConfig(String configFileName) throws IOException {
		String remotePath = getRemoteFlinkRoot() + "/" + configFileName;
		String localPath = getLocalFlinkRoot() + "/" + configFileName;
		File configFile = new File(localPath);
		if (configFile.exists()) {
			configFile.delete();
		}
		download(remotePath, localPath);
		String contents = Files.toString(configFile, StandardCharsets.UTF_8);
		configs = JsonConverter.fromJson(contents,
			new TypeReference <Map <String, PluginDownloaderConfig>>() {}.getType());
		isConfigLoaded = true;
	}

	public List <String> listAvailablePlugins() throws IOException {
		if (!isConfigLoaded) {
			loadConfig();
		}
		return new ArrayList <>(configs.keySet());
	}

	public List <String> listAvailablePluginVersions(String pluginName) throws IOException {
		if (!isConfigLoaded) {
			loadConfig();
		}
		if (configs.containsKey(pluginName)) {
			Map <String, List <String>> allVersions = configs.get(pluginName).versions;
			return new ArrayList <>(allVersions.keySet());
		} else {
			throw new IllegalArgumentException("plugin [" + pluginName + "] not found!");
		}
	}

	private List <String> getListOfJars(String pluginName, String pluginVersion) throws IOException {
		if (!isConfigLoaded) {
			loadConfig();
		}
		if (configs.containsKey(pluginName)) {
			Map <String, List <String>> versions = configs.get(pluginName).versions;
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
		if (!isConfigLoaded) {
			loadConfig();
		}
		if (configs.containsKey(pluginName)) {
			return configs.get(pluginName).defaultVersion;
		} else {
			throw new IllegalArgumentException("plugin [" + pluginName + "] not found!");
		}
	}

	public void downloadPlugin(String pluginName, String pluginVersion) throws IOException {
		if (!isConfigLoaded) {
			loadConfig();
		}
		List <String> jars = getListOfJars(pluginName, pluginVersion);
		String remotePath = getRemoteFlinkRoot() + "/" + pluginName + "-" + pluginVersion + "/";
		String localPath = getLocalFlinkRoot() + "/" + pluginName + "-" + pluginVersion + "/";
		for (String jar : jars) {
			download(remotePath + jar, localPath + jar);
		}
	}

	public void downloadPlugin(String pluginName) throws IOException {
		if (!isConfigLoaded) {
			loadConfig();
		}
		downloadPlugin(pluginName, getDefaultPluginVersion(pluginName));
	}

	private void download(String url, String localPath) throws IOException {
		URL httpurl = new URL(url);
		File f = new File(localPath);
		if (!f.exists()) {
			if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
				System.out.println(String.format("Downloading %s to %s", httpurl, localPath));
			}
			FileUtils.copyURLToFile(httpurl, f);
		} else {
			if (AlinkGlobalConfiguration.isPrintProcessInfo()) {
				System.out.println(String.format("%s already downloaded to %s", httpurl, localPath));
			}
		}
	}

	public void downloadAll() throws IOException {
		List <String> plugins = this.listAvailablePlugins();
		for (String plugin : plugins) {
			this.downloadPlugin(plugin);
		}
	}

	private void deletePlugin(String pluginName, String pluginVersion) throws IOException {
		String localPath = getLocalFlinkRoot() + "/" + pluginName + "-" + pluginVersion;
		File pluginDir = new File(localPath);
		if (pluginDir.exists()) {
			FileUtils.deleteDirectory(pluginDir);
		}
	}

	private void backupPlugin(String pluginName, String pluginVersion) throws IOException {
		String localPath = getLocalFlinkRoot() + "/" + pluginName + "-" + pluginVersion;
		File pluginDir = new File(localPath);
		pluginDir.renameTo(new File(localPath + ".old"));
	}

	/**
	 * Scans all existing plugins and re-download them more carefully.
	 *
	 * @throws IOException
	 */
	public void upgrade() throws IOException {
		loadConfig();
		File root = new File(getLocalFlinkRoot());
		File[] pluginFiles = root.listFiles();

		// backup and download all plugins
		for (int i = 0; i < pluginFiles.length; i++) {
			String pluginDirName = pluginFiles[i].getName();
			int dashIndex = pluginDirName.indexOf("-");
			if (dashIndex == -1) {
				// file is "config.json"
				continue;
			}
			String pluginName = pluginDirName.substring(0, dashIndex);
			String pluginVersion = pluginDirName.substring(dashIndex + 1);
			backupPlugin(pluginName, pluginVersion);
			downloadPlugin(pluginName, pluginVersion);
		}

		// delete all backups
		for (int i = 0; i < pluginFiles.length; i++) {
			String pluginDirName = pluginFiles[i].getName();
			int dashIndex = pluginDirName.indexOf("-");
			if (dashIndex == -1) {
				// file is "config.json"
				continue;
			}
			String pluginName = pluginDirName.substring(0, dashIndex);
			String pluginVersion = pluginDirName.substring(dashIndex + 1);
			deletePlugin(pluginName, pluginVersion + ".old");
		}
	}
}
