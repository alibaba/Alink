package com.alibaba.alink.common.io.filesystem.binary;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;

public class BinaryRecordReader implements Serializable {

	private InputStream stream;
	private RowSerializer serializer;
	private transient int firstByte;
	private transient int secondByte;
	private transient int thirdByte;

	public BinaryRecordReader(InputStream stream, String[] fieldNames, TypeInformation[] fieldTypes) {
		this.stream = stream;
		this.serializer = new RowSerializer(fieldNames, fieldTypes);
	}

	public void readAndCheckHeader() throws IOException {
		firstByte = stream.read();
		secondByte = stream.read();
		thirdByte = stream.read();
		if (firstByte != BinaryRecordWriter.MAGIC1
			|| secondByte != BinaryRecordWriter.MAGIC2
			|| thirdByte != BinaryRecordWriter.MAGIC3) {
			throw new RuntimeException("Illegal alink format, header mismatch");
		}
	}

	public boolean hasNextRecord() throws IOException {
		firstByte = stream.read();
		secondByte = stream.read();
		thirdByte = stream.read();
		return firstByte >= 0 && secondByte >= 0 && thirdByte >= 0;
	}

	public Row getNextRecord() throws IOException {
		int len = (firstByte << 16) + (secondByte << 8) + thirdByte;
		Preconditions.checkArgument(len <= BinaryRecordWriter.MAX_RECORD_LENGTH);
		byte[] bytes = new byte[len];
		int start = 0;
		while (len > 0) {
			int n = stream.read(bytes, start, len);
			if (n < 0) {
				throw new RuntimeException("unexpected end of stream.");
			}
			start += n;
			len -= n;
		}
		return serializer.deserialize(bytes);
	}
}
