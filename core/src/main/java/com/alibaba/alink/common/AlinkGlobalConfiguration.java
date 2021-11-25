package com.alibaba.alink.common;

import org.apache.flink.runtime.util.EnvironmentInformation;

import com.alibaba.alink.common.io.plugin.PluginConfig;
import com.alibaba.alink.common.io.plugin.PluginDownloader;

public final class AlinkGlobalConfiguration {

	private static boolean printProcessInfo = false;

	private static String pluginDir = PluginConfig.DEFAULT_FLINK_PLUGINS_DIRS;

	private static boolean autoPluginDownload = true;

	private static final PluginDownloader pluginDownloader = new PluginDownloader();

	public synchronized static boolean isPrintProcessInfo() {
		return printProcessInfo;
	}

	public static void setPrintProcessInfo(boolean printProcessInfo) {
		AlinkGlobalConfiguration.printProcessInfo = printProcessInfo;
	}

	public synchronized static void setPluginDir(String pluginDir) {
		AlinkGlobalConfiguration.pluginDir = pluginDir;
	}

	/**
	 * This method is in conflict with {@link PluginConfig}. We should fix this in the next few releases.
	 */
	public synchronized static String getPluginDir() {
		String pluginDir = System.getenv(PluginConfig.ENV_FLINK_PLUGINS_DIR);

		if (pluginDir != null) {
			return pluginDir;
		}

		pluginDir = System.getProperty(PluginConfig.ENV_FLINK_PLUGINS_DIR);

		if (pluginDir != null) {
			return pluginDir;
		}

		return AlinkGlobalConfiguration.pluginDir;
	}

	public synchronized static void setAutoPluginDownload(boolean autoPluginDownload) {
		AlinkGlobalConfiguration.autoPluginDownload = autoPluginDownload;
	}

	public synchronized static boolean getAutoPluginDownload() {
		String localAutoPluginDownloadStr = System.getenv("ALINK_AUTO_PLUGIN_DOWNLOAD");

		if (localAutoPluginDownloadStr != null) {
			return Boolean.parseBoolean(localAutoPluginDownloadStr);
		}

		localAutoPluginDownloadStr = System.getProperty("ALINK_AUTO_PLUGIN_DOWNLOAD");

		if (localAutoPluginDownloadStr != null) {
			return Boolean.parseBoolean(localAutoPluginDownloadStr);
		}

		return autoPluginDownload;
	}

	public static String getFlinkVersion() {
		String flinkVersion = EnvironmentInformation.getVersion();
		int lastDotIndex = flinkVersion.lastIndexOf(".");

		if (lastDotIndex < 0) {
			return flinkVersion;
		}

		return String.format("flink-%s", flinkVersion.substring(0, lastDotIndex));
	}

	public static PluginDownloader getPluginDownloader() {
		return pluginDownloader;
	}
}
