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

package com.alibaba.flink.ml.coding.impl;

import com.alibaba.flink.ml.cluster.node.MLContext;
import com.alibaba.flink.ml.coding.Coding;
import com.alibaba.flink.ml.coding.CodingException;

/**
 * coding object with byte[] format.
 */
public class ByteArrayCodingImpl implements Coding<byte[]> {
	private MLContext context;

	public ByteArrayCodingImpl(MLContext context) {
		this.context = context;
	}

	@Override
	public byte[] decode(byte[] bytes) throws CodingException {
		return bytes;
	}

	@Override
	public byte[] encode(byte[] object) throws CodingException {
		return object;
	}
}
