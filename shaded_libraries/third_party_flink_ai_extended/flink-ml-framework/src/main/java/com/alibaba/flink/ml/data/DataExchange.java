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

package com.alibaba.flink.ml.data;

import com.alibaba.flink.ml.cluster.node.MLContext;
import com.alibaba.flink.ml.coding.Decoding;
import com.alibaba.flink.ml.coding.Encoding;
import com.alibaba.flink.ml.coding.CodingFactory;

import java.io.IOException;

/**
 * data exchange util between java and python.
 * @param <IN> write object class.
 * @param <OUT> read object class.
 */
public class DataExchange<IN, OUT> {

	private MLContext mlContext;
	private DataBridge dataBridge;
	private RecordReader recordReader;
	private RecordWriter recordWriter;
	private Encoding<IN> encoding;
	private Decoding<OUT> decoding;

	public DataExchange(MLContext mlContext) {
		this.mlContext = mlContext;
		try {
			recordReader = RecordFactory.getRecordRecord(mlContext);
			recordWriter = RecordFactory.getRecordWriter(mlContext);
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		try {
			dataBridge = DataBridgeFactory.getDataBridge(mlContext);
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		try {
			encoding = CodingFactory.getEncoding(mlContext);
			decoding = CodingFactory.getDecoding(mlContext);
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	/**
	 * write java object to python process.
	 * @param obj java object.
	 * @return true: write success, false: write failed.
	 * @throws IOException
	 */
	public boolean write(IN obj) throws IOException {
		return dataBridge.write(obj, recordWriter, encoding);
	}

	/**
	 * read object from python process.
	 * @param readWait: true: blocking, false: no blocking.
	 * @return read object.
	 * @throws IOException
	 */
	public OUT read(boolean readWait) throws IOException {
		return dataBridge.read(recordReader, readWait, decoding);
	}

	public long getReadRecords() {
		return dataBridge.getReadRecords();
	}

	public long getWriteRecords() {
		return dataBridge.getWriteRecords();
	}

	public MLContext getMlContext() {
		return mlContext;
	}

	public DataBridge getDataBridge() {
		return dataBridge;
	}

	public RecordReader getRecordReader() {
		return recordReader;
	}

	public RecordWriter getRecordWriter() {
		return recordWriter;
	}

	public Encoding<IN> getEncoding() {
		return encoding;
	}

	public Decoding<OUT> getDecoding() {
		return decoding;
	}
}

