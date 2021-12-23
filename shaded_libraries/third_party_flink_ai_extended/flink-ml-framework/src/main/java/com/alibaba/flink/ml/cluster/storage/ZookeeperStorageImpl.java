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
import com.alibaba.flink.ml.util.MLException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * storage interface implement with zookeeper.
 */
public class ZookeeperStorageImpl implements Storage {

	private static final Logger LOG = LoggerFactory.getLogger(ZookeeperStorageImpl.class);

	private CuratorFramework client;

	static Map<Pair<String, String>, Pair<CuratorFramework, Integer>> map = new ConcurrentHashMap<>();
	private static final int DEFAULT_ZK_TIMEOUT = 6000;
	private static final Duration SESSION_TIMEOUT = Duration.ofMinutes(1);

	private static synchronized CuratorFramework getCuratorFramework(String connectString, String basePath,
			Map<String, String> properties) {
		Pair<String, String> p = Pair.of(connectString, basePath);
		Pair<CuratorFramework, Integer> f = map.get(p);
		if (f != null) {
			Pair<CuratorFramework, Integer> f2 = Pair.of(f.getLeft(), f.getRight() + 1);
			map.put(p, f2);
			return f.getKey();
		}

		String timeoutStr = properties
				.getOrDefault(MLConstants.CONFIG_ZOOKEEPER_TIMEOUT, String.valueOf(DEFAULT_ZK_TIMEOUT));
		int timeOut = Integer.valueOf(timeoutStr);

		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 5);

		//curator namespace can't start with "/", so trim it
		String basePath2 = basePath.startsWith("/") ? basePath.substring(1) : basePath;
		CuratorFramework c = CuratorFrameworkFactory.builder().
				connectString(connectString).
				sessionTimeoutMs((int) SESSION_TIMEOUT.toMillis()).
				connectionTimeoutMs(timeOut).
				retryPolicy(retryPolicy).
				namespace(basePath2).
				build();
		c.start();
		LOG.info("Create a new ZK connection. ConnectionStr=" + connectString + ", basePath=" + basePath);
		map.put(p, Pair.of(c, 1));
		return c;
	}

	private static synchronized void returnCuratorFramework(CuratorFramework c) {
		Pair<String, String> key = null;
		Pair<CuratorFramework, Integer> val = null;
		for (Map.Entry<Pair<String, String>, Pair<CuratorFramework, Integer>> entry : map.entrySet()) {
			if (entry.getValue().getKey().equals(c)) {
				key = entry.getKey();
				int v = entry.getValue().getValue();
				v--;
				val = Pair.of(entry.getValue().getLeft(), v);
				break;
			}
		}
		if (key != null) {
			if (val.getRight() <= 0) {
				map.remove(key);
				val.getKey().close();
				LOG.info("Close ZK connection.");
			} else {
				map.put(key, val);
			}
		}
	}


	public ZookeeperStorageImpl(String connectString, String basePath, Map<String, String> properties)
			throws IOException {
		client = getCuratorFramework(connectString, basePath, properties);
	}

	public void start() {
	}

	@Override
	public byte[] getValue(String path) throws IOException {
		String realPath = "/" + path;
		try {
			client.sync().forPath(realPath);
			byte[] res = client.getData().forPath(realPath);
			return res;
		} catch (KeeperException.NoNodeException ex) {
			return null;
		} catch (Exception e) {
			throw new MLException("Failed to get value for path " + path, e);
		}
	}

	@Override
	public void setValue(String path, byte[] value) throws IOException {
		String realPath = "/" + path;
		try {
			Stat stat = client.checkExists().forPath(realPath);
			if (stat != null) {
				LOG.info(path + " is replaced with new value.");
				client.setData().forPath(realPath, value);
			} else {
				client.create().creatingParentsIfNeeded().forPath(realPath, value);
			}
		} catch (Exception e) {
			throw new MLException("Fail to create zookeeper node " + path, e);
		}
	}

	@Override
	public void removeValue(String path) throws IOException {
		String realPath = "/" + path;
		try {
			client.delete().deletingChildrenIfNeeded().forPath(realPath);
		} catch (KeeperException.NoNodeException ne) {
			// do nothing
		} catch (Exception e) {
			throw new MLException("Fail to delete node " + path, e);
		}
	}

	@Override
	public List<String> listChildren(String path) throws IOException {
		String realPath = "/" + path;
		try {
			return client.getChildren().forPath(realPath);
		} catch (KeeperException.NoNodeException ex) {
			return new ArrayList<>();
		} catch (Exception e) {
			throw new MLException("Failed to list children for path " + path, e);
		}
	}

	@Override
	public boolean exists(String path) throws IOException {
		String realPath = "/" + path;
		try {
			Stat stat = client.checkExists().forPath(realPath);
			if (null == stat) {
				return false;
			} else {
				return true;
			}
		} catch (Exception e) {
			throw new MLException("Fail to check path existence " + path, e);
		}
	}

	@Override
	public void close() {
		returnCuratorFramework(client);
	}

	@Override
	public void clear() {
		try {
			client.delete().guaranteed().deletingChildrenIfNeeded().forPath("/");
		} catch (Exception e) {
			LOG.warn("Failed to delete ZK node {}", client.getNamespace(), e);
		}
	}
}
