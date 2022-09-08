package com.alibaba.alink.common.io.filesystem.binary;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import com.alibaba.alink.common.io.filesystem.binary.BinaryRecordWriter.RecordWriterV1;
import org.apache.commons.io.IOUtils;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;

public class BinaryRecordReader implements Serializable {

	private static final long serialVersionUID = -8789744031720381820L;
	private final InputStream stream;

	private final String[] fieldNames;

	private final TypeInformation<?>[] fieldTypes;

	private transient RecordReader recordReader;

	public BinaryRecordReader(InputStream stream, String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		this.stream = stream;

		this.fieldNames = fieldNames;
		this.fieldTypes = fieldTypes;
	}

	public static boolean checkHeaderV1(int firstByte, int secondByte, int thirdByte) {
		return firstByte == BinaryRecordWriter.MAGIC1
			&& secondByte == BinaryRecordWriter.MAGIC2
			&& thirdByte == BinaryRecordWriter.MAGIC3;
	}

	public static boolean checkHeaderV2(int firstByte, int secondByte, int thirdByte) {
		return firstByte == BinaryRecordWriter.MAGIC1_V2
			&& secondByte == BinaryRecordWriter.MAGIC2_V2
			&& thirdByte == BinaryRecordWriter.MAGIC3_V2;
	}

	public void readAndCheckHeader() throws IOException {
		int firstByte = stream.read();
		int secondByte = stream.read();
		int thirdByte = stream.read();

		if (checkHeaderV1(firstByte, secondByte, thirdByte)) {
			recordReader = new RecordReaderV1(new DataInputStream(stream), new RowSerializer(fieldNames, fieldTypes));
			return;
		}

		if (checkHeaderV2(firstByte, secondByte, thirdByte)) {
			recordReader = new RecordReaderV2(new DataInputStream(stream), new RowSerializerV2(fieldNames, fieldTypes));
			return;
		}

		throw new AkIllegalDataException("Illegal alink format, header mismatch");
	}

	interface RecordReader {
		boolean hasNextRecord() throws IOException;
		Row getNextRecord() throws IOException;
	}

	public static class RecordReaderV1 implements RecordReader {
		private final DataInputStream dataInputStream;

		private final RowSerializer serializer;

		private transient int firstByte;
		private transient int secondByte;
		private transient int thirdByte;

		public RecordReaderV1(DataInputStream dataInputStream, RowSerializer serializer) {
			this.dataInputStream = dataInputStream;
			this.serializer = serializer;
		}

		@Override
		public boolean hasNextRecord() throws IOException {
			firstByte = dataInputStream.read();
			secondByte = dataInputStream.read();
			thirdByte = dataInputStream.read();
			return firstByte >= 0 && secondByte >= 0 && thirdByte >= 0;
		}

		@Override
		public Row getNextRecord() throws IOException {
			if (firstByte == RecordWriterV1.MAX_BYTE
				&& secondByte == RecordWriterV1.MAX_BYTE
				&& thirdByte == RecordWriterV1.MAX_BYTE) {

				int len = dataInputStream.readInt();
				byte[] bytes = new byte[len];
				IOUtils.readFully(dataInputStream, bytes, 0, len);

				return serializer.deserialize(bytes);
			}

			int len = (firstByte << 16) + (secondByte << 8) + thirdByte;
			byte[] bytes = new byte[len];
			int start = 0;
			while (len > 0) {
				int n = dataInputStream.read(bytes, start, len);
				if (n < 0) {
					throw new AkUnclassifiedErrorException("unexpected end of stream.");
				}
				start += n;
				len -= n;
			}
			return serializer.deserialize(bytes);
		}
	}

	public static class RecordReaderV2 implements RecordReader {
		private final DataInputStream dataInputStream;

		private final RowSerializerV2 serializer;

		private transient int len;

		public RecordReaderV2(DataInputStream dataInputStream, RowSerializerV2 serializer) {
			this.dataInputStream = dataInputStream;
			this.serializer = serializer;
		}

		@Override
		public boolean hasNextRecord() throws IOException {
			int b0 = dataInputStream.read();

			if (b0 < 0) {
				return false;
			}

			int b1 = dataInputStream.read();

			if (b1 < 0) {
				return false;
			}

			int b2 = dataInputStream.read();

			if (b2 < 0) {
				return false;
			}

			int b3 = dataInputStream.read();

			if (b3 < 0) {
				return false;
			}

			len = (
				((b0 & 0xFF) << 24)
				+ ((b1 & 0xFF) << 16)
				+ ((b2 & 0xFF) << 8)
				+ (b3 & 0xFF)
			);

			return len >= 0;
		}

		@Override
		public Row getNextRecord() throws IOException {
			byte[] bytes = new byte[len];
			IOUtils.readFully(dataInputStream, bytes, 0, len);
			return serializer.deserialize(bytes);
		}
	}

	public boolean hasNextRecord() throws IOException {
		return recordReader != null && recordReader.hasNextRecord();
	}

	public Row getNextRecord() throws IOException {
		return recordReader.getNextRecord();
	}
}
