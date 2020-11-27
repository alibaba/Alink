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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

public class PluginManager {

	private final PluginDirectory pluginDirectory;

	/**
	 * Parent-classloader to all classloader that are used for plugin loading. We expect that this is thread-safe.
	 */
	private final ClassLoader parentClassLoader;

	/**
	 * List of patterns for classes that should always be resolved from the parent ClassLoader.
	 */
	private final String[] alwaysParentFirstPatterns;

	public PluginManager(PluginDirectory pluginDirectory, String[] alwaysParentFirstPatterns) {
		this(pluginDirectory, Configuration.class.getClassLoader(), alwaysParentFirstPatterns);
	}

	public PluginManager(PluginDirectory pluginDirectory, ClassLoader parentClassLoader,
						 String[] alwaysParentFirstPatterns) {
		this.pluginDirectory = pluginDirectory;
		this.parentClassLoader = parentClassLoader;
		this.alwaysParentFirstPatterns = alwaysParentFirstPatterns;
	}

	public <P> Iterator <Tuple2 <P, PluginDescriptor>> load(
		Class <P> service, String flinkVersion, String pluginName, String pluginVersion) throws IOException {

		PluginDescriptor pluginDescriptor = pluginDirectory.createPluginDescriptorForSubDirectory(
			flinkVersion, pluginName, pluginVersion
		);

		PluginLoader pluginLoader = PluginLoader.create(pluginDescriptor, parentClassLoader,
			alwaysParentFirstPatterns);

		return new Iterator <Tuple2 <P, PluginDescriptor>>() {
			final Iterator <P> serviceIter = pluginLoader.load(service);

			@Override
			public boolean hasNext() {
				return serviceIter.hasNext();
			}

			@Override
			public Tuple2 <P, PluginDescriptor> next() {
				return Tuple2.of(serviceIter.next(), pluginDescriptor);
			}
		};
	}

	@Override
	public String toString() {
		return "PluginManager{" +
			"parentClassLoader=" + parentClassLoader +
			", pluginDirectory=" + pluginDirectory +
			", alwaysParentFirstPatterns=" + Arrays.toString(alwaysParentFirstPatterns) +
			'}';
	}
}
