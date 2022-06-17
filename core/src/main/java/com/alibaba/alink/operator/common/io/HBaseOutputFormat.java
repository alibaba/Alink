package com.alibaba.alink.operator.common.io;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.io.hbase.HBase;
import com.alibaba.alink.common.io.hbase.HBaseClassLoaderFactory;
import com.alibaba.alink.common.io.plugin.TemporaryClassLoaderContext;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.params.io.HBaseSinkParams;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class HBaseOutputFormat extends RichOutputFormat <Row> {

	private static final int START_SIZE_OUTPUT_VIEW = 8 * 1024 * 1024;

	private final Params params;
	private final String[] colNames;
	private final String tableName;
	private final String familyName;
	private final TypeInformation <?>[] colTypes;
	private final HBaseClassLoaderFactory factory;
	private final String[] valueColNames;

	private transient int[] rowkeyColIndices;
	private transient int[] valColIndices;

	private transient HBase hbase;
	private transient TypeSerializer[] serializer;
	private transient DataOutputSerializer outputView;

	public HBaseOutputFormat(
		Params params, String[] colNames, TypeInformation <?>[] colTypes) {

		this.params = params;
		this.colNames = colNames;
		this.colTypes = colTypes;
		this.tableName = params.get(HBaseSinkParams.HBASE_TABLE_NAME);
		this.valueColNames = params.get(HBaseSinkParams.VALUE_COLS);
		this.familyName = params.get(HBaseSinkParams.HBASE_FAMILY_NAME);

		this.factory = new HBaseClassLoaderFactory(params.get(HBaseSinkParams.PLUGIN_VERSION));
	}

	@Override
	public void configure(Configuration parameters) {
		// pass
	}

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {

		try (TemporaryClassLoaderContext context = TemporaryClassLoaderContext.of(factory.create())) {
			hbase = HBaseClassLoaderFactory.create(factory).create(params);
		}

		TableSchema dataSchema = new TableSchema(colNames, colTypes);

		String[] rowkeyCols = params.get(HBaseSinkParams.HBASE_ROWKEY_COLS);

		rowkeyColIndices = TableUtil.findColIndicesWithAssertAndHint(dataSchema, rowkeyCols);
		valColIndices = TableUtil.findColIndicesWithAssertAndHint(dataSchema, this.valueColNames);

		ExecutionConfig executionConfig = new ExecutionConfig();
		TypeInformation[] valueTypes = TableUtil.findColTypesWithAssertAndHint(dataSchema, this.valueColNames);
		serializer = new TypeSerializer[valueTypes.length];
		for (int i = 0; i < valueTypes.length; i++) {
			serializer[i] = valueTypes[i].createSerializer(executionConfig);
		}

		outputView = new DataOutputSerializer(START_SIZE_OUTPUT_VIEW);
	}

	private byte[] serialize(Object o, int index) throws IOException {
		serializer[index].serialize(o, outputView);
		int length = outputView.length();
		byte[] value = Arrays.copyOfRange(outputView.getSharedBuffer(), 0, length);
		outputView.clear();
		return value;
	}

	private String mergeRowKey(Row record) {
		StringBuffer buffer = new StringBuffer();
		for (int i = 0; i < this.rowkeyColIndices.length; i++) {
			buffer.append(record.getField(this.rowkeyColIndices[i]));
		}
		return buffer.toString();
	}

	@Override
	public void writeRecord(Row record) throws IOException {
		Map <String, byte[]> dataMap = new HashMap <>(valColIndices.length);
		for (int i = 0; i < valColIndices.length; i++) {
			dataMap.put(this.valueColNames[i], serialize(record.getField(valColIndices[i]), i));
		}
		hbase.set(this.tableName, mergeRowKey(record), this.familyName, dataMap);
	}

	@Override
	public void close() throws IOException {
		if (hbase != null) {
			hbase.close();
		}
	}
}
