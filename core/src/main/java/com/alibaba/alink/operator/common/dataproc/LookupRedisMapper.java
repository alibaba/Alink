package com.alibaba.alink.operator.common.dataproc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.io.filesystem.binary.RowSerializer;
import com.alibaba.alink.common.io.redis.RedisClassLoaderFactory;
import com.alibaba.alink.common.io.redis.Redis;
import com.alibaba.alink.common.mapper.Mapper;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.params.dataproc.LookupRedisParams;

/**
 * Mapper for Key to Values operation.
 */
public class LookupRedisMapper extends Mapper {
	private final RedisClassLoaderFactory factory;

	private transient RowSerializer keyRowSerializer;
	private transient RowSerializer valueRowSerializer;
	private transient Redis redis;

	public LookupRedisMapper(TableSchema dataSchema, Params params) {
		super(dataSchema, params);

		factory = new RedisClassLoaderFactory(params.get(LookupRedisParams.PLUGIN_VERSION));
	}

	@Override
	public void open() {
		super.open();
		TableSchema valuesSchema = TableUtil.schemaStr2Schema(params.get(LookupRedisParams.OUTPUT_SCHEMA_STR));
		String[] selectedColNames = params.get(LookupRedisParams.SELECTED_COLS);
		keyRowSerializer = new RowSerializer(
			selectedColNames,
			TableUtil.findColTypesWithAssertAndHint(getDataSchema(), selectedColNames)
		);

		valueRowSerializer = new RowSerializer(
			valuesSchema.getFieldNames(),
			valuesSchema.getFieldTypes()
		);

		redis = RedisClassLoaderFactory.create(factory).create(params);
	}

	@Override
	public void close() {
		super.close();

		if (redis != null) {
			redis.close();
		}
	}

	@Override
	protected void map(SlicedSelectedSample selection, SlicedResult result) throws Exception {
		Row key = new Row(selection.length());
		for (int i = 0; i < selection.length(); ++i) {
			key.setField(i, selection.get(i));
		}
		byte[] r = redis.get(keyRowSerializer.serialize(key));
		if (null == r) {
			for (int i = 0; i < result.length(); ++i) {
				result.set(i, null);
			}
		} else {
			Row row = valueRowSerializer.deserialize(r);
			for (int i = 0; i < result.length(); ++i) {
				result.set(i, row.getField(i));
			}
		}
	}

	@Override
	protected Tuple4 <String[], String[], TypeInformation <?>[], String[]> prepareIoSchema(
		TableSchema dataSchema, Params params) {
		TableSchema valuesSchema = TableUtil.schemaStr2Schema(params.get(LookupRedisParams.OUTPUT_SCHEMA_STR));

		return Tuple4.of(params.get(LookupRedisParams.SELECTED_COLS),
			valuesSchema.getFieldNames(), valuesSchema.getFieldTypes(),
			params.get(LookupRedisParams.RESERVED_COLS));
	}
}
