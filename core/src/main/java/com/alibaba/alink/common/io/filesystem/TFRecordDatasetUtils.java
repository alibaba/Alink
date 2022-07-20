package com.alibaba.alink.common.io.filesystem;

import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.dl.data.TFRecordReader;
import com.alibaba.alink.common.dl.data.TFRecordWriter;
import com.alibaba.alink.common.dl.utils.TFExampleConversionUtils;
import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.exceptions.AkUnclassifiedErrorException;
import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import com.alibaba.alink.common.io.filesystem.copy.FileInputFormat;
import com.alibaba.alink.common.io.filesystem.copy.FileOutputFormat;
import com.alibaba.alink.common.utils.TableUtil;
import org.tensorflow.proto.example.Example;

import java.io.DataOutputStream;
import java.io.IOException;

public class TFRecordDatasetUtils {
	public static class TFRecordDatasetInputFormat extends FileInputFormat <Row> {
		private final String schemaStr;
		private transient TableSchema schema;
		private transient boolean isInactiveSplit;
		private transient TFRecordReader reader;
		private transient byte[] bytes;

		public TFRecordDatasetInputFormat(FilePath filePath, String schemaStr) {
			super(filePath.getPath(), filePath.getFileSystem());
			this.schemaStr = schemaStr;
		}

		@Override
		public void open(FileInputSplit fileSplit) throws IOException {
			isInactiveSplit = fileSplit.getStart() > 0;
			if (isInactiveSplit) {
				return;
			}
			super.open(fileSplit);
			schema = TableUtil.schemaStr2Schema(schemaStr);
			reader = new TFRecordReader(stream, true);
		}

		@Override
		public boolean reachedEnd() throws IOException {
			if (isInactiveSplit) {
				return true;
			}
			bytes = reader.read();
			return null == bytes;
		}

		@Override
		public Row nextRecord(Row reuse) throws IOException {
			if (null == bytes) {
				bytes = reader.read();
				if (null == bytes) {
					return null;
				}
			}
			Example example = Example.parseFrom(bytes);
			//noinspection deprecation
			return TFExampleConversionUtils.fromExample(example, schema.getFieldNames(), schema.getFieldTypes());
		}
	}

	public static class TFRecordDatasetOutputFormat extends FileOutputFormat <Row> {
		private final String schemaStr;
		private transient TableSchema schema;
		private transient TFRecordWriter writer;

		public TFRecordDatasetOutputFormat(FilePath filePath, String schemaStr, WriteMode writeMode) {
			super(filePath.getPath(), filePath.getFileSystem());
			this.schemaStr = schemaStr;
			setWriteMode(writeMode);

			// hack for initial stage of local filesystem.
			if (filePath.getFileSystem().isDistributedFS()) {
				return;
			}
			// check if path exists
			try {
				if (filePath.getFileSystem().exists(filePath.getPath())) {
					// path exists, check write mode
					switch (writeMode) {

						case NO_OVERWRITE:
							// file or directory may not be overwritten
							throw new AkIllegalOperatorParameterException(
								"File or directory already exists. Existing files and directories are not overwritten "
									+ "in "
									+
									WriteMode.NO_OVERWRITE.name() + " mode. Use " + WriteMode.OVERWRITE.name() +
									" mode to overwrite existing files and directories.");

						case OVERWRITE:
							// output path exists. We delete it and all contained files in case of a directory.
							filePath.getFileSystem().delete(filePath.getPath(), true);
							break;

						default:
							throw new AkUnsupportedOperationException("Invalid write mode: " + writeMode);
					}
				}
			} catch (IOException e) {
				throw new AkUnclassifiedErrorException(
					String.format("Failed to handle overwrite on file: %s", filePath.getPath()), e);
			}
		}

		@Override
		public void open(int taskNumber, int numTasks) throws IOException {
			super.open(taskNumber, numTasks);
			schema = TableUtil.schemaStr2Schema(schemaStr);
			writer = new TFRecordWriter(new DataOutputStream(stream));
		}

		@Override
		public void writeRecord(Row record) throws IOException {
			//noinspection deprecation
			Example example = TFExampleConversionUtils.toExample(record,
				schema.getFieldNames(), schema.getFieldTypes());
			byte[] bytes = example.toByteArray();
			writer.write(bytes);
		}
	}
}
