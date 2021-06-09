package com.alibaba.alink.common.io.filesystem.binary;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;

/**
 * The file format:
 * <p>
 * header(3bytes)
 * record1_length(3bytes) record1_data(`record1_length` bytes)
 * record2_length(3bytes) record2_data(`record2_length` bytes)
 * ...
 * recordn_length(3bytes) recordn_data(`recordn_length` bytes)
 */
public class BinaryRecordWriter implements Serializable {

	static final int MAGIC1 = 'A';
	static final int MAGIC2 = 'L';
	static final int MAGIC3 = 'K';
	private static final long serialVersionUID = -2186048394178435538L;

	private final OutputStream stream;
	private final RowSerializer serializer;

	public BinaryRecordWriter(OutputStream stream, String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		this.stream = stream;
		this.serializer = new RowSerializer(fieldNames, fieldTypes);
	}

	public void writeHeader() throws IOException {
		stream.write(MAGIC1);
		stream.write(MAGIC2);
		stream.write(MAGIC3);
	}

	public void writeRecord(Row record) throws IOException {
		byte[] bytes = serializer.serialize(record);
		int len = bytes.length;
		stream.write(len >> 16);
		stream.write(len >> 8);
		stream.write(len);
		stream.write(bytes);
	}
}
