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

import java.io.Closeable;
import java.io.IOException;

/**
 * date exchange write record interface.
 */
public interface RecordWriter extends Closeable {
	/**
	 * write a record to python process.
	 * @param record write record.
	 * @param offset record offset.
	 * @param length write record length.
	 * @return true: write success, false: write failed.
	 * @throws IOException
	 */
	boolean write(byte[] record, int offset, int length) throws IOException;

	/**
	 * read a record from python process.
	 * @param record write record.
	 * @return true: write success, false: write failed.
	 * @throws IOException
	 */
	boolean write(byte[] record) throws IOException;
}
