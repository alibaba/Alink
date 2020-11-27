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

import java.net.URL;
import java.util.Arrays;

/**
 * Descriptive meta information for a plugin.
 */
public class PluginDescriptor {

	/** Unique identifier of the plugin. */
	private final String pluginId;

	/** URLs to the plugin resources code. Usually this contains URLs of the jars that will be loaded for the plugin. */
	private final URL[] pluginResourceURLs;

	private final String[] allowedFlinkPackages;

	private final String version;

	public PluginDescriptor(String pluginId, URL[] pluginResourceURLs, String[] allowedFlinkPackages, final String version) {
		this.pluginId = pluginId;
		this.pluginResourceURLs = pluginResourceURLs;
		this.allowedFlinkPackages = allowedFlinkPackages;
		this.version = version;
	}

	public String getPluginId() {
		return pluginId;
	}

	public URL[] getPluginResourceURLs() {
		return pluginResourceURLs;
	}

	public String[] getAllowedFlinkPackages() {
		return allowedFlinkPackages;
	}

	public String getVersion() {
		return version;
	}

	@Override
	public String toString() {
		return "PluginDescriptor{" +
			"pluginId='" + pluginId + '\'' +
			", pluginResourceURLs=" + Arrays.toString(pluginResourceURLs) +
			", loaderExcludePatterns=" + Arrays.toString(allowedFlinkPackages) +
			", version=" + version +
			'}';
	}
}
