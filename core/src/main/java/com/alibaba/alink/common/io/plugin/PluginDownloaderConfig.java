package com.alibaba.alink.common.io.plugin;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

public class PluginDownloaderConfig {
	@JsonProperty("default-version")
	public String defaultVersion;
	Map <String, List <String>> versions;
}
