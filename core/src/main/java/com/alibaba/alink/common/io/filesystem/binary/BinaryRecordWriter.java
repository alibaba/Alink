package com.alibaba.alink.common.io.filesystem.binary;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;

import java.io.DataOutputStream;
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

	static final int MAGIC1_V2 = 'F';

	static final int MAGIC2_V2 = 'L';

	static final int MAGIC3_V2 = 'K';

	enum FormatVersion {
		V1,
		V2
	}

	private static final long serialVersionUID = -2186048394178435538L;

	private final RecordWriter recordWriter;

	public BinaryRecordWriter(OutputStream stream, String[] fieldNames, TypeInformation <?>[] fieldTypes) {
		recordWriter = new RecordWriterV1(stream, fieldNames, fieldTypes);
	}

	public void writeHeader() throws IOException {
		recordWriter.writeHeader();
	}

	public void writeRecord(Row record) throws IOException {
		recordWriter.writeRecord(record);
	}

	public interface RecordWriter extends Serializable {
		void writeHeader() throws IOException;

		void writeRecord(Row record) throws IOException;
	}

	public static class RecordWriterV1 implements RecordWriter {

		public static final int MAX_THREE_BYTE_INT = 0xFFFFFF;

		public static final int MAX_BYTE = 0x0000FF;

		private final DataOutputStream dataOutputStream;

		private final RowSerializer serializer;

		public RecordWriterV1(OutputStream stream, String[] fieldNames, TypeInformation <?>[] fieldTypes) {
			this.dataOutputStream = new DataOutputStream(stream);
			this.serializer = new RowSerializer(fieldNames, fieldTypes);
		}

		@Override
		public void writeHeader() throws IOException {
			dataOutputStream.write(MAGIC1);
			dataOutputStream.write(MAGIC2);
			dataOutputStream.write(MAGIC3);
		}

		@Override
		public void writeRecord(Row record) throws IOException {
			byte[] bytes = serializer.serialize(record);
			int len = bytes.length;

			if (len >= MAX_THREE_BYTE_INT) {
				dataOutputStream.write(MAX_BYTE);
				dataOutputStream.write(MAX_BYTE);
				dataOutputStream.write(MAX_BYTE);

				dataOutputStream.writeInt(len);
				dataOutputStream.write(bytes);
				return;
			}

			dataOutputStream.write(len >> 16);
			dataOutputStream.write(len >> 8);
			dataOutputStream.write(len);
			dataOutputStream.write(bytes);
		}
	}

	public static class RecordWriterV2 implements RecordWriter {
		private final DataOutputStream dataOutputStream;

		private final RowSerializerV2 serializer;

		public RecordWriterV2(OutputStream stream, String[] fieldNames, TypeInformation <?>[] fieldTypes) {
			this.dataOutputStream = new DataOutputStream(stream);
			this.serializer = new RowSerializerV2(fieldNames, fieldTypes);
		}

		@Override
		public void writeHeader() throws IOException {
			dataOutputStream.writeByte(MAGIC1_V2);
			dataOutputStream.writeByte(MAGIC2_V2);
			dataOutputStream.writeByte(MAGIC3_V2);
		}

		@Override
		public void writeRecord(Row record) throws IOException {
			byte[] bytes = serializer.serialize(record);
			dataOutputStream.writeInt(bytes.length);
			dataOutputStream.write(bytes);
		}
	}
}
