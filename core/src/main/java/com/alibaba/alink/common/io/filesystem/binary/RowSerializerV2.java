package com.alibaba.alink.common.io.filesystem.binary;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.MTable.MTableKryoSerializerV2;
import com.alibaba.alink.common.linalg.tensor.Tensor;
import com.alibaba.alink.common.linalg.tensor.TensorKryoSerializer;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

public class RowSerializerV2 implements BaseRowSerializer, Serializable {

	private static final long serialVersionUID = -542406479129743102L;
	private static final int START_SIZE_OUTPUT_VIEW = 8 * 1024 * 1024;

	private final TypeSerializer <Row> serializer;
	private final DataOutputSerializer outputView;
	private final DataInputDeserializer inputView;

	public RowSerializerV2(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes, fieldNames);
		ExecutionConfig executionConfig = new ExecutionConfig();
		executionConfig.addDefaultKryoSerializer(MTable.class, new MTableKryoSerializerV2());
		executionConfig.addDefaultKryoSerializer(Tensor.class, new TensorKryoSerializer());
		this.serializer = rowTypeInfo.createSerializer(executionConfig);
		this.outputView = new DataOutputSerializer(START_SIZE_OUTPUT_VIEW);
		this.inputView = new DataInputDeserializer();
	}

	@Override
	public byte[] serialize(Row row) throws IOException {
		serializer.serialize(row, outputView);
		int length = outputView.length();
		byte[] ret = Arrays.copyOfRange(outputView.getSharedBuffer(), 0, length);
		outputView.clear();
		return ret;
	}

	@Override
	public Row deserialize(byte[] bytes) throws IOException {
		inputView.setBuffer(bytes, 0, bytes.length);
		return serializer.deserialize(inputView);
	}
}
