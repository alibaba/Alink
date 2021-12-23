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

package com.alibaba.flink.ml.tensorflow.storage;

import com.alibaba.flink.ml.cluster.storage.Storage;

import java.io.IOException;
import java.util.List;

public class DummyStorage implements Storage {
	@Override
	public byte[] getValue(String path) throws IOException {
		return null;
	}

	@Override
	public void setValue(String path, byte[] value) throws IOException {
	}

	@Override
	public void removeValue(String path) throws IOException {
	}

	@Override
	public List<String> listChildren(String path) throws IOException {
		return null;
	}

	@Override
	public boolean exists(String path) throws IOException {
		return false;
	}

	@Override
	public void clear() {
	}
}
