package com.alibaba.alink.common;

import org.apache.flink.runtime.util.EnvironmentInformation;

import com.alibaba.alink.common.io.plugin.PluginDownloader;

public final class AlinkGlobalConfiguration {

	private static boolean printProcessInfo = false;

	private static String pluginDir = "plugins";

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

	public synchronized static String getPluginDir() {
		return pluginDir;
	}

	public static String getFlinkVersion() {
		String flinkVersion = EnvironmentInformation.getVersion();
		int lastDotIndex = flinkVersion.lastIndexOf(".");

		if (lastDotIndex < 0) {
			return flinkVersion;
		}

		return String.format("flink-%s", flinkVersion.substring(0, lastDotIndex));
	}

	public static PluginDownloader getPluginDownloader(){
		return pluginDownloader;
	}
}
