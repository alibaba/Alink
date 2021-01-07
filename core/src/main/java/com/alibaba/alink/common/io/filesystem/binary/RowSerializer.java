package com.alibaba.alink.common.io.filesystem.binary;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

public class RowSerializer implements Serializable {

	private static final long serialVersionUID = -542406479129743102L;
	private static final int START_SIZE_OUTPUT_VIEW = 8 * 1024 * 1024;

	private final TypeSerializer <Row> serializer;
	private final DataOutputSerializer outputView;
	private final DataInputDeserializer inputView;

	public RowSerializer(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes, fieldNames);
		this.serializer = rowTypeInfo.createLegacySerializer(new ExecutionConfig());
		this.outputView = new DataOutputSerializer(START_SIZE_OUTPUT_VIEW);
		this.inputView = new DataInputDeserializer();
	}

	public byte[] serialize(Row row) throws IOException {
		serializer.serialize(row, outputView);
		int length = outputView.length();
		byte[] ret = Arrays.copyOfRange(outputView.getSharedBuffer(), 0, length);
		outputView.clear();
		return ret;
	}

	public Row deserialize(byte[] bytes) throws IOException {
		inputView.setBuffer(bytes, 0, bytes.length);
		return serializer.deserialize(inputView);
	}
}
