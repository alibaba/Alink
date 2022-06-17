package com.alibaba.alink.operator.common.dataproc;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.io.hbase.HBase;
import com.alibaba.alink.common.io.hbase.HBaseClassLoaderFactory;
import com.alibaba.alink.common.io.plugin.TemporaryClassLoaderContext;
import com.alibaba.alink.common.mapper.Mapper;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.params.dataproc.LookupHBaseParams;

import java.io.IOException;
import java.util.Map;

/**
 * Mapper for Key to Values operation.
 */
public class LookupHBaseMapper extends Mapper {
	private final HBaseClassLoaderFactory factory;

	private transient TypeSerializer <Object>[] serializers;
	private transient DataInputDeserializer inputView;
	private transient HBase hbase;
	private final String[] colNames;
	private final String tableName;
	private final String familyName;

	public LookupHBaseMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
		factory = new HBaseClassLoaderFactory(params.get(LookupHBaseParams.PLUGIN_VERSION));
		tableName = params.get(LookupHBaseParams.HBASE_TABLE_NAME);
		familyName = params.get(LookupHBaseParams.HBASE_FAMILY_NAME);
		TableSchema valuesSchema = TableUtil.schemaStr2Schema(params.get(LookupHBaseParams.OUTPUT_SCHEMA_STR));
		this.colNames = valuesSchema.getFieldNames();
	}

	@Override
	public void open() {
		super.open();
		this.inputView = new DataInputDeserializer();
		TableSchema valuesSchema = TableUtil.schemaStr2Schema(params.get(LookupHBaseParams.OUTPUT_SCHEMA_STR));
		TypeInformation[] types = valuesSchema.getFieldTypes();
		serializers = new TypeSerializer[types.length];
		ExecutionConfig executionConfig = new ExecutionConfig();
		for (int i = 0; i < types.length; i++) {
			serializers[i] = types[i].createSerializer(executionConfig);
		}
		try (TemporaryClassLoaderContext context = TemporaryClassLoaderContext.of(factory.create())) {
			hbase = HBaseClassLoaderFactory.create(factory).create(params);
		} catch (IOException e) {
			throw new RuntimeException("LookupHBaseMapper hbase initialization error, message is " + e.getMessage());
		}

	}

	@Override
	public void close() {
		super.close();

		if (hbase != null) {
			try {
				hbase.close();
			} catch (IOException e) {
				throw new RuntimeException("LookupHBaseMapper hbase close error, message is " + e.getMessage());
			}
		}
	}

	private String mergeRowKey(SlicedSelectedSample selection) {
		StringBuffer buffer = new StringBuffer();
		for (int i = 0; i < selection.length(); i++) {
			buffer.append(selection.get(i));
		}
		return buffer.toString();
	}

	@Override
	protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		String rowkey = mergeRowKey(selection);
		Map <String, byte[]> map = hbase.getFamilyColumns(this.tableName, rowkey, this.familyName);
		for (int i = 0; i < this.colNames.length; i++) {
			String colName = this.colNames[i];
			if (map.containsKey(colName)) {
				byte[] value = map.get(colName);
				inputView.setBuffer(value, 0, value.length);
				result.set(i, this.serializers[i].deserialize(inputView));
			} else {
				result.set(i, null);
			}
		}
	}

	@Override
	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(
		TableSchema dataSchema, Params params) {
		TableSchema valuesSchema = TableUtil.schemaStr2Schema(params.get(LookupHBaseParams.OUTPUT_SCHEMA_STR));

		return Tuple4.of(params.get(LookupHBaseParams.HBASE_ROWKEY_COLS),
			valuesSchema.getFieldNames(), valuesSchema.getFieldTypes(),
			params.get(LookupHBaseParams.RESERVED_COLS));
	}
}
