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

import java.io.IOException;
import java.util.List;

/**
 * kv store abstraction
 */
public interface Storage {
	/**
	 * get vale by key.
	 * @param path key.
	 * @return value.
	 * @throws IOException
	 */
	byte[] getValue(String path) throws IOException;

	/**
	 * set a value to a key.
	 * @param path key.
	 * @param value
	 * @throws IOException
	 */
	void setValue(String path, byte[] value) throws IOException;

	/**
	 * remove a key.
	 * @param path path key.
	 * @throws IOException
	 */
	void removeValue(String path) throws IOException;

	/**
	 * list a key's children.
	 * @param path key.
	 * @return a key path children.
	 * @throws IOException
	 */
	List<String> listChildren(String path) throws IOException;

	/**
	 * @param path key
	 * @return true: exist key, false: not exist key.
	 * @throws IOException
	 */
	boolean exists(String path) throws IOException;

	/**
	 * close all resources.
	 */
	default void close() {
	}

	/**
	 * clear all key.
	 */
	void clear();
}
