package com.alibaba.alink.common.io.filesystem.binary;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.MTable.MTableKryoSerializerV2;
import com.alibaba.alink.common.linalg.tensor.Tensor;
import com.alibaba.alink.common.linalg.tensor.TensorKryoSerializer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

public class RowStreamSerializerV2 implements BaseStreamRowSerializer, Serializable {

	private static final long serialVersionUID = -542406479129743102L;

	private final TypeSerializer <Row> serializer;
	private final DataInputViewStreamWrapper inputView;
	private final DataOutputViewStreamWrapper outputView;

	public RowStreamSerializerV2(
		String[] fieldNames, TypeInformation <?>[] fieldTypes,
		InputStream boundInputStream, OutputStream boundOutputStream) {
		RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes, fieldNames);
		ExecutionConfig executionConfig = new ExecutionConfig();
		executionConfig.addDefaultKryoSerializer(MTable.class, new MTableKryoSerializerV2());
		executionConfig.addDefaultKryoSerializer(Tensor.class, new TensorKryoSerializer());
		serializer = rowTypeInfo.createSerializer(executionConfig);

		inputView = boundInputStream == null ? null : new DataInputViewStreamWrapper(boundInputStream);
		outputView = boundOutputStream == null ? null : new DataOutputViewStreamWrapper(boundOutputStream);
	}

	@Override
	public void serialize(Row row) throws IOException {
		serializer.serialize(row, outputView);
	}

	@Override
	public Row deserialize() throws IOException {
		return serializer.deserialize(inputView);
	}
}
