package com.alibaba.alink.common;

import com.alibaba.alink.common.io.plugin.PluginDownloader;

public final class AlinkGlobalConfiguration {

	private static boolean batchPredictMultiThread = false;

	private static boolean streamPredictMultiThread = false;

	private static boolean printProcessInfo = false;

	private static String pluginDir = "plugins";

	private static final PluginDownloader pluginDownloader = new PluginDownloader();

	public static boolean isBatchPredictMultiThread() {
		return batchPredictMultiThread;
	}

	public static void setBatchPredictMultiThread(boolean batchPredictMultiThread) {
		AlinkGlobalConfiguration.batchPredictMultiThread = batchPredictMultiThread;
	}

	public static boolean isStreamPredictMultiThread() {
		return streamPredictMultiThread;
	}

	public static void setStreamPredictMultiThread(boolean streamPredictMultiThread) {
		AlinkGlobalConfiguration.streamPredictMultiThread = streamPredictMultiThread;
	}

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
		return "flink-1.11";
	}

	public static PluginDownloader getPluginDownloader(){
		return pluginDownloader;
	}
}
