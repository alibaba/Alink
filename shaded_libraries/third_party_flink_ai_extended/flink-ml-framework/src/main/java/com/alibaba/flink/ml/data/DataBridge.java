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

import com.alibaba.flink.ml.coding.Decoding;
import com.alibaba.flink.ml.coding.Encoding;

import java.io.Closeable;
import java.io.IOException;

/**
 * data bridge between java and python process.
 */
public interface DataBridge extends Closeable {
	/**
	 * write object to python.
	 * @param obj java object.
	 * @param recordWriter writer.
	 * @param encoding encoding object.
	 * @param <T> java object class
	 * @return true: write success false: write failed.
	 * @throws IOException
	 */
	<T> boolean write(T obj, RecordWriter recordWriter, Encoding<T> encoding) throws IOException;

	/**
	 * read from python process.
	 * @param recordReader reader object.
	 * @param readWait true: blocking read, false: no blocking read.
	 * @param decoding decoding object.
	 * @param <T> java object class.
	 * @return read object.
	 * @throws IOException
	 */
	<T> T read(RecordReader recordReader, boolean readWait, Decoding<T> decoding) throws IOException;

	/**
	 * @return read records number.
	 */
	long getReadRecords();

	/**
	 * @return write records number.
	 */
	long getWriteRecords();
}
