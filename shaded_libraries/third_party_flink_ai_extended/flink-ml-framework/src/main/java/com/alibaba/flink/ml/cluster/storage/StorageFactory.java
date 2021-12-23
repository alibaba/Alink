/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.flink.ml.cluster.storage;

import com.alibaba.flink.ml.util.MLConstants;
import com.alibaba.flink.ml.util.ReflectUtil;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

/**
 * a factory for store interface.
 */
public class StorageFactory {
	private static final Logger LOG = LoggerFactory.getLogger(StorageFactory.class);

	public static Storage memoryStorage = new MemoryStorageImpl();

	public static Storage getStorageInstance(Map<String, String> propMap) {
		String storageType = propMap.getOrDefault(MLConstants.CONFIG_STORAGE_TYPE,
				MLConstants.STORAGE_MEMORY);
		if (storageType.equalsIgnoreCase(MLConstants.STORAGE_ZOOKEEPER)) {
			LOG.info("storage use zookeeper.");
			String connStr = propMap.getOrDefault(MLConstants.CONFIG_ZOOKEEPER_CONNECT_STR, "");
			if (connStr.isEmpty()) {
				throw new RuntimeException("config " + MLConstants.CONFIG_ZOOKEEPER_CONNECT_STR + " is empty!");
			}
			ZookeeperStorageImpl zk = null;
			try {
				String basePath = getZKStorageBasePath(propMap);
				zk = new ZookeeperStorageImpl(connStr, basePath, propMap);
				LOG.info("Zookeeper connection=" + connStr + ", basePath=" + basePath);
			} catch (IOException e) {
				LOG.error("Fail to get zookeeper storage.", e);
				throw new RuntimeException(e.getMessage(), e);
			}
			zk.start();
			return zk;
		} else if (storageType.equals(MLConstants.STORAGE_CUSTOM)) {
			String className = propMap.get(MLConstants.STORAGE_IMPL_CLASS);
			Preconditions.checkNotNull(className, "Implementation class name is needed for custom storage type");
			try {
				return ReflectUtil.createInstance(className, new Class[0], new Object[0]);
			} catch (InstantiationException | InvocationTargetException | NoSuchMethodException |
					IllegalAccessException | ClassNotFoundException e) {
				LOG.error("Failed to create custom storage", e);
				throw new RuntimeException(e);
			}
		}
		return memoryStorage;
	}

	public static String getZKStorageBasePath(Map<String, String> propMap) {
		String version = propMap.get(MLConstants.JOB_VERSION);
		return propMap.getOrDefault(MLConstants.CONFIG_ZOOKEEPER_BASE_PATH, "") + "/" + version;
	}
}
