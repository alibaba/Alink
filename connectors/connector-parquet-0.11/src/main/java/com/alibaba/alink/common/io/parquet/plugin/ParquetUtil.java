package com.alibaba.alink.common.io.parquet.plugin;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.utils.ParquetSchemaConverter;
import org.apache.flink.shaded.guava18.com.google.common.collect.BiMap;
import org.apache.flink.shaded.guava18.com.google.common.collect.HashBiMap;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.io.filesystem.BaseFileSystem;
import com.alibaba.alink.common.io.filesystem.FilePath;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ParquetUtil {
	private static final BiMap<PrimitiveTypeName, TypeInformation<?>> PRIMITIVE_TYPE_MAP = HashBiMap.create();

	static {
		PRIMITIVE_TYPE_MAP.put(PrimitiveTypeName.BOOLEAN, Types.BOOLEAN);
		PRIMITIVE_TYPE_MAP.put(PrimitiveTypeName.BINARY, Types.STRING);
		PRIMITIVE_TYPE_MAP.put(PrimitiveTypeName.INT32, Types.INT);
		PRIMITIVE_TYPE_MAP.put(PrimitiveTypeName.INT64, Types.LONG);
		PRIMITIVE_TYPE_MAP.put(PrimitiveTypeName.INT96, Types.SQL_TIMESTAMP);
		PRIMITIVE_TYPE_MAP.put(PrimitiveTypeName.DOUBLE, Types.DOUBLE);
		PRIMITIVE_TYPE_MAP.put(PrimitiveTypeName.FLOAT, Types.FLOAT);
	}

	public static MessageType getReadSchemaFromParquetFile(FilePath filePath) throws IOException {
		MessageType messageType = readSchemaFromFile(filePath);
		RowTypeInfo schema = (RowTypeInfo) ParquetSchemaConverter.fromParquetType(messageType);
		List<String[]> paths = messageType.getPaths();
		List<Type> types = new ArrayList<>();

		for (int i = 0; i < paths.size(); i++) {
			String[] path = paths.get(i);
			Type type = messageType.getType(path);
			if(PRIMITIVE_TYPE_MAP.containsKey(type.asPrimitiveType().getPrimitiveTypeName())){
				types.add(type);
			}
		}
		MessageType readMessageType = new MessageType("alink_parquet_source",types);
		return readMessageType;
	}

	public static TableSchema getTableSchemaFromParquetFile(FilePath filePath) throws IOException {
		MessageType messageType = getReadSchemaFromParquetFile(filePath);
		RowTypeInfo schema = (RowTypeInfo) ParquetSchemaConverter.fromParquetType(messageType);
		return new TableSchema(schema.getFieldNames(),schema.getFieldTypes());
	}

	;

	public static MessageType readSchemaFromFile(FilePath filePath) throws IOException {
		BaseFileSystem fs = filePath.getFileSystem();
		Path path = filePath.getPath();
		FileStatus pathFile = fs.getFileStatus(path);

		if (pathFile.isDir()) {
			for (FileStatus fileStatus : fs.listStatus(path)) {
				MessageType messageType = readSchemaFromFile(new FilePath(fileStatus.getPath(), fs));
				if (messageType != null) {
					return messageType;
				}
			}
		} else {
			if(!path.getName().endsWith(".parquet")) {
				return null;
			}
			try (ParquetFileReader fileReader
					 = new ParquetFileReader(new ParquetInputFile(filePath),
				ParquetReadOptions.builder().build())) {
				return fileReader.getFileMetaData().getSchema();
			}
		}
		return null;
	}

	public static class ParquetInputFile implements InputFile {
		private FilePath filePath;

		public ParquetInputFile(FilePath filePath) throws IOException {
			this.filePath = filePath;
		}

		@Override
		public long getLength() throws IOException {
			return filePath.getFileSystem().getFileStatus(filePath.getPath()).getLen();
		}

		@Override
		public SeekableInputStream newStream() throws IOException {
			return new MyDelegatingSeekableInputStream(filePath.getFileSystem().open(filePath.getPath()));
		}

		private static class MyDelegatingSeekableInputStream extends DelegatingSeekableInputStream {

			private final FSDataInputStream fsDataInputStream;

			public MyDelegatingSeekableInputStream(FSDataInputStream fsDataInputStream) {
				super(fsDataInputStream);

				this.fsDataInputStream = fsDataInputStream;
			}

			@Override
			public long getPos() throws IOException {
				return fsDataInputStream.getPos();
			}

			@Override
			public void seek(long newPos) throws IOException {
				fsDataInputStream.seek(newPos);
			}
		}
	}
}
