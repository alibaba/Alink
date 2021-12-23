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

package com.alibaba.flink.ml.data.impl;

import com.alibaba.flink.ml.cluster.node.MLContext;
import com.alibaba.flink.ml.coding.Decoding;
import com.alibaba.flink.ml.coding.Encoding;
import com.alibaba.flink.ml.data.DataBridge;
import com.alibaba.flink.ml.data.RecordReader;
import com.alibaba.flink.ml.data.RecordWriter;

import java.io.IOException;

/**
 * a common data bridge implement.
 */
public class DataBridgeImpl implements DataBridge {
	private MLContext mlContext;
	private long readRecords = 0;
	private long writeRecords = 0;

	public DataBridgeImpl(MLContext mlContext) {
		this.mlContext = mlContext;
	}

	@Override
	public <T> boolean write(T obj, RecordWriter recordWriter, Encoding<T> encoding) throws IOException {
		writeRecords++;
		return recordWriter.write(encoding.encode(obj));
	}

	@Override
	public <T> T read(RecordReader recordReader, boolean readWait, Decoding<T> decoding) throws IOException {
		byte[] res;
		if (readWait) {
			res = recordReader.read();
		} else {
			res = recordReader.tryRead();
		}
		if (null == res) {
			return null;
		} else {
			readRecords++;
			return decoding.decode(res);
		}
	}

	@Override
	public long getReadRecords() {
		return readRecords;
	}

	@Override
	public long getWriteRecords() {
		return writeRecords;
	}

	@Override
	public void close() throws IOException {

	}
}
