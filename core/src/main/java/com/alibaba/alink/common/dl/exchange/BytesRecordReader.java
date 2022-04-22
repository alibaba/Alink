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

package com.alibaba.alink.common.dl.exchange;

import com.alibaba.flink.ml.data.RecordReader;
import com.alibaba.flink.ml.util.SpscOffHeapQueue;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * a RecordReader only reads bytes.
 */
public class BytesRecordReader implements RecordReader {

	private final DataInputStream input;
	private boolean eof = false;

	public BytesRecordReader(SpscOffHeapQueue.QueueInputStream in) {
		this.input = new DataInputStream(in);
	}

	public byte[] tryRead() throws IOException {
		if (input.available() > 0) {
			return read();
		}
		return null;
	}

	public boolean isReachEOF() {
		return eof;
	}

	public byte[] read() throws IOException {
		/*
		  Record format:
		  uint32 length
		  byte   data[length]
		 */
		byte[] lenBytes = new byte[4];
		int lenRead;
		try {
			// Only catch EOF here, other case means corrupted file
			lenRead = input.read(lenBytes);
			if (lenRead < 0) {
				eof = true;
				return null;
			}
		} catch (EOFException eofException) {
			eof = true;
			return null; // return null means EOF
		}
		if (lenRead < lenBytes.length) {
			input.readFully(lenBytes, lenRead, lenBytes.length - lenRead);
		}
		long len = fromInt32LE(lenBytes);

		byte[] data = new byte[(int) len];
		input.readFully(data);
		return data;
	}

	private int fromInt32LE(byte[] data) {
		assert data.length == 4;
		ByteBuffer bb = ByteBuffer.wrap(data);
		bb.order(ByteOrder.LITTLE_ENDIAN);
		return bb.getInt();
	}

	@Override
	public void close() throws IOException {
	}
}
