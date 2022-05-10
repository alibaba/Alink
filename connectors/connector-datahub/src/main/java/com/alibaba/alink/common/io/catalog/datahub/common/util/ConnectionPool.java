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

package com.alibaba.alink.common.io.catalog.datahub.common.util;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A managed connection pool, the {@code ConnectionPool} can used as a static field
 * to shared across all threads.
 *
 * @param <T> the pooled connection, such as {@code HTablePool} or {@code DruidDataSource}
 */
public class ConnectionPool<T> {

	private final Map<String, T> pools = new ConcurrentHashMap<>();
	private final Map<String, Integer> referenceCounts = new ConcurrentHashMap<>();

	public synchronized boolean contains(String dataSourceName) {
		return pools.containsKey(dataSourceName);
	}

	public synchronized T get(String dataSourceName) {
		referenceCounts.put(dataSourceName, referenceCounts.get(dataSourceName) + 1);
		return pools.get(dataSourceName);
	}

	public synchronized T put(String dataSourceName, T dataSource) {
		referenceCounts.put(dataSourceName, 1);
		return pools.put(dataSourceName, dataSource);
	}

	public synchronized boolean remove(String dataSourceName) {
		Integer count = referenceCounts.get(dataSourceName);
		if (count == null) {
			// no connection existed, shouldn't close the pooled connection
			return false;
		} else if (count == 1) {
			referenceCounts.remove(dataSourceName);
			pools.remove(dataSourceName);
			// should close the pooled connection
			return true;
		} else {
			referenceCounts.put(dataSourceName, count - 1);
			// other thread is still using the connection,
			// shouldn't close the pooled connection
			return false;
		}
	}

	public synchronized int size() {
		return pools.size();
	}
}
