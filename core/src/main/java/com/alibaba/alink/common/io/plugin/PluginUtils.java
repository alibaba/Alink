/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.alink.common.io.plugin;

import org.apache.flink.configuration.Configuration;

import com.alibaba.alink.common.io.filesystem.FilePath;

import java.nio.file.Paths;
import java.util.Map;

/**
 * Utility functions for the plugin mechanism.
 */
public final class PluginUtils {

	private PluginUtils() {
		throw new AssertionError("Singleton class.");
	}

	public static JarsPluginManager createJarsPluginManagerFromRootFolder(Configuration configuration) {
		return createJarsPluginManagerFromRootFolder(PluginConfig.fromConfiguration(configuration));
	}

	private static JarsPluginManager createJarsPluginManagerFromRootFolder(PluginConfig pluginConfig) {
		if (pluginConfig.getPluginsPath().isPresent()) {
			return new JarsPluginManager(
				new JarsPluginDirectory(pluginConfig.getPluginsPath().get()),
				pluginConfig.getAlwaysParentFirstPatterns()
			);
		} else {
			return new JarsPluginManager(
				new JarsPluginDirectory(Paths.get(PluginConfig.DEFAULT_ALINK_PLUGINS_DIRS)),
				pluginConfig.getAlwaysParentFirstPatterns()
			);
		}
	}

	public static ResourcesPluginManager createResourcesPluginManagerFromRootFolder(Configuration configuration) {
		return createResourcesPluginManagerFromRootFolder(PluginConfig.fromConfiguration(configuration));
	}

	private static ResourcesPluginManager createResourcesPluginManagerFromRootFolder(PluginConfig pluginConfig) {
		if (pluginConfig.getPluginsPath().isPresent()) {
			return new ResourcesPluginManager(
				new ResourcesPluginDirectory(new FilePath(pluginConfig.getPluginsPath().get().toString()))
			);
		} else {
			return new ResourcesPluginManager(
				new ResourcesPluginDirectory(
					new FilePath(Paths.get(PluginConfig.DEFAULT_ALINK_PLUGINS_DIRS).toString()))
			);
		}
	}

	public static Configuration readPluginConf(Map <String, String> context) {

		Configuration configuration;

		if (context.isEmpty()) {
			// Run in flink console, user should set the plugin follow the configuration of flink.
			configuration = org.apache.flink.configuration.GlobalConfiguration.loadConfiguration().clone();
		} else {
			// Run in Local and RemoteEnv in PyAlink
			configuration = new Configuration();

			for (Map.Entry <String, String> entry : context.entrySet()) {
				configuration.setString(entry.getKey(), entry.getValue());
			}
		}

		return configuration;
	}
}
