package com.alibaba.alink.common.io.plugin;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.*;

public class PluginDownloaderTest {

	String jsonString = "{\n"
		+ "  \"oss\": {\n"
		+ "    \"versions\": {\n"
		+ "      \"1.10.0\": [\n"
		+ "        \"shaded_flink_oss_fs_hadoop-1.10.0-0.2.jar\"\n"
		+ "      ],\n"
		+ "      \"1.11.0\": [\n"
		+ "        \"shaded_flink_oss_fs_hadoop-1.10.0-0.2.jar\"\n"
		+ "      ]\n"
		+ "    },\n"
		+ "    \"default-version\": \"1.10.0\"\n"
		+ "  },\n"
		+ "  \"hive\": {\n"
		+ "    \"versions\": {\n"
		+ "      \"2.0.1\": [\n"
		+ "        \"antlr-runtime-3.5.2.jar\",\n"
		+ "        \"flink-connector-hive-with-batch-0.1-1.10.0-SNAPSHOT.jar\",\n"
		+ "        \"hive-metastore-2.3.5.jar\",\n"
		+ "        \"datanucleus-api-jdo-4.2.4.jar\",\n"
		+ "        \"flink-hadoop-compatibility_2.11-1.10.0.jar\",\n"
		+ "        \"jackson-core-2.6.5.jar\",\n"
		+ "        \"datanucleus-core-4.1.17.jar\",\n"
		+ "        \"flink-shaded-hadoop-2-uber-2.8.3-10.0.jar\",\n"
		+ "        \"javax.jdo-3.2.0-m3.jar\",\n"
		+ "        \"datanucleus-rdbms-4.1.19.jar\",\n"
		+ "        \"hive-exec-2.3.5.jar\",\n"
		+ "        \"jdo-api-3.0.1.jar\",\n"
		+ "        \"commons-dbcp-1.4.jar\",\n"
		+ "        \"commons-dbcp2-2.0.1.jar\",\n"
		+ "        \"commons-pool-1.5.4.jar\",\n"
		+ "        \"commons-pool2-2.2.jar\",\n"
		+ "        \"bonecp-0.8.0.RELEASE.jar\"\n"
		+ "      ],\n"
		+ "      \"2.3.4\": [\n"
		+ "        \"antlr-runtime-3.5.2.jar\",\n"
		+ "        \"flink-connector-hive-with-batch-0.1-1.10.0-SNAPSHOT.jar\",\n"
		+ "        \"hive-metastore-2.3.5.jar\",\n"
		+ "        \"datanucleus-api-jdo-4.2.4.jar\",\n"
		+ "        \"flink-hadoop-compatibility_2.11-1.10.0.jar\",\n"
		+ "        \"jackson-core-2.6.5.jar\",\n"
		+ "        \"datanucleus-core-4.1.17.jar\",\n"
		+ "        \"flink-shaded-hadoop-2-uber-2.8.3-10.0.jar\",\n"
		+ "        \"javax.jdo-3.2.0-m3.jar\",\n"
		+ "        \"datanucleus-rdbms-4.1.19.jar\",\n"
		+ "        \"hive-exec-2.3.5.jar\",\n"
		+ "        \"jdo-api-3.0.1.jar\",\n"
		+ "        \"commons-dbcp-1.4.jar\",\n"
		+ "        \"commons-dbcp2-2.0.1.jar\",\n"
		+ "        \"commons-pool-1.5.4.jar\",\n"
		+ "        \"commons-pool2-2.2.jar\",\n"
		+ "        \"bonecp-0.8.0.RELEASE.jar\"\n"
		+ "      ]\n"
		+ "    },\n"
		+ "    \"default-version\": \"2.0.1\"\n"
		+ "  }\n"
		+ "}\n";
	PluginDownloader pluginDownloader = AlinkGlobalConfiguration.getPluginDownloader();

	@Test
	public void testListAvailableVersions() throws IOException {
		pluginDownloader.loadConfigFromString(jsonString);
		List <String> hiveVersion = pluginDownloader.listAvailablePluginVersions("hive");
		assertEquals(hiveVersion.get(0), "2.0.1");
		assertEquals(hiveVersion.get(1), "2.3.4");
	}

	@Test
	public void testListAvailablePlugins() throws IOException {
		pluginDownloader.loadConfigFromString(jsonString);
		List <String> allPlugins = pluginDownloader.listAvailablePlugins();
		assertEquals(allPlugins.get(0), "oss");
		assertEquals(allPlugins.get(1), "hive");
	}

	//@Test
	//public void testDownloadPlugin() throws IOException {
	//	pluginDownloader.downloadPlugin("hive", "2.0.1");
	//}
	//
	//@Test
	//public void downloadAll() throws IOException {
	//	pluginDownloader.downloadAll();
	//}
	//
	//@Test
	//public void upgrade() throws IOException {
	//	pluginDownloader.upgrade();
	//}
}