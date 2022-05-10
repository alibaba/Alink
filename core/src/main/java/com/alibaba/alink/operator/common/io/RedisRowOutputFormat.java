package com.alibaba.alink.operator.common.io;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.io.filesystem.binary.RowSerializer;
import com.alibaba.alink.common.io.redis.Redis;
import com.alibaba.alink.common.io.redis.RedisClassLoaderFactory;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.params.io.RedisParams;
import com.alibaba.alink.params.io.RedisRowSinkParams;
import org.apache.commons.lang3.ArrayUtils;

import java.io.IOException;

public class RedisRowOutputFormat extends RichOutputFormat <Row> {

	private final Params params;
	private final String[] colNames;
	private final TypeInformation <?>[] colTypes;
	private final RedisClassLoaderFactory factory;

	private transient int[] keyColIndices;
	private transient int[] valColIndices;

	private transient Redis redis;
	private transient RowSerializer keyRowSerializer;
	private transient RowSerializer valueRowSerializer;

	public RedisRowOutputFormat(
		Params params, String[] colNames, TypeInformation <?>[] colTypes) {

		this.params = params;
		this.colNames = colNames;
		this.colTypes = colTypes;

		this.factory = new RedisClassLoaderFactory(params.get(RedisParams.PLUGIN_VERSION));
	}

	@Override
	public void configure(Configuration parameters) {
		// pass
	}

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {

		redis = RedisClassLoaderFactory.create(factory).create(params);

		TableSchema dataSchema = new TableSchema(colNames, colTypes);

		String[] keyColNames = params.get(RedisRowSinkParams.KEY_COLS);
		String[] valueColNames = params.get(RedisRowSinkParams.VALUE_COLS);

		if (null == valueColNames) {
			valueColNames = ArrayUtils.removeElements(dataSchema.getFieldNames(), keyColNames);
		}

		keyColIndices = TableUtil.findColIndicesWithAssertAndHint(dataSchema, keyColNames);
		keyRowSerializer = new RowSerializer(
			keyColNames,
			TableUtil.findColTypesWithAssertAndHint(dataSchema, keyColNames)
		);

		valColIndices = TableUtil.findColIndicesWithAssertAndHint(dataSchema, valueColNames);
		valueRowSerializer = new RowSerializer(
			valueColNames,
			TableUtil.findColTypesWithAssertAndHint(dataSchema, valueColNames)
		);
	}

	@Override
	public void writeRecord(Row record) throws IOException {
		redis.set(
			keyRowSerializer.serialize(Row.project(record, keyColIndices)),
			valueRowSerializer.serialize(Row.project(record, valColIndices))
		);
	}

	@Override
	public void close() throws IOException {
		if (redis != null) {
			redis.close();
		}
	}
}
