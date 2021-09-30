/* Copyright 2016 The TensorFlow Authors. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
==============================================================================*/

package com.alibaba.alink.common.dl.data;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * This file is just a copy of {@link org.tensorflow.hadoop.util.TFRecordReader}
 */
public class TFRecordReader {
	private final InputStream input;
	private final boolean crcCheck;

	public TFRecordReader(InputStream input, boolean crcCheck) {
		this.input = input;
		this.crcCheck = crcCheck;
	}

	public byte[] read() throws IOException {
		/**
		 * TFRecord format:
		 * uint64 length
		 * uint32 masked_crc32_of_length
		 * byte   data[length]
		 * uint32 masked_crc32_of_data
		 */
		byte[] lenBytes = new byte[8];
		try {
			// Only catch EOF here, other case means corrupted file
			readFully(input, lenBytes);
		} catch (EOFException eof) {
			return null; // return null means EOF
		}
		Long len = fromInt64LE(lenBytes);

		// Verify length crc32
		if (!crcCheck) {
			input.skip(4);
		} else {
			byte[] lenCrc32Bytes = new byte[4];
			readFully(input, lenCrc32Bytes);
			int lenCrc32 = fromInt32LE(lenCrc32Bytes);
			if (lenCrc32 != Crc32C.maskedCrc32c(lenBytes)) {
				throw new IOException("Length header crc32 checking failed: " + lenCrc32 + " != " +
					Crc32C.maskedCrc32c(lenBytes) + ", length = " + len);
			}
		}

		if (len > Integer.MAX_VALUE) {
			throw new IOException("Record size exceeds max value of int32: " + len);
		}
		byte[] data = new byte[len.intValue()];
		readFully(input, data);

		// Verify data crc32
		if (!crcCheck) {
			input.skip(4);
		} else {
			byte[] dataCrc32Bytes = new byte[4];
			readFully(input, dataCrc32Bytes);
			int dataCrc32 = fromInt32LE(dataCrc32Bytes);
			if (dataCrc32 != Crc32C.maskedCrc32c(data)) {
				throw new IOException("Data crc32 checking failed: " + dataCrc32 + " != " +
					Crc32C.maskedCrc32c(data));
			}
		}
		return data;
	}

	private long fromInt64LE(byte[] data) {
		assert data.length == 8;
		ByteBuffer bb = ByteBuffer.wrap(data);
		bb.order(ByteOrder.LITTLE_ENDIAN);
		return bb.getLong();
	}

	private int fromInt32LE(byte[] data) {
		assert data.length == 4;
		ByteBuffer bb = ByteBuffer.wrap(data);
		bb.order(ByteOrder.LITTLE_ENDIAN);
		return bb.getInt();
	}

	private void readFully(InputStream in, byte[] buffer) throws IOException {
		int nbytes;
		for(int nread = 0; nread < buffer.length; nread += nbytes) {
			nbytes = in.read(buffer, nread, buffer.length - nread);
			if (nbytes < 0) {
				throw new EOFException("End of file reached before reading fully.");
			}
		}
	}
}
