package com.alibaba.alink.operator.common.io;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.AlinkTypes;
import com.alibaba.alink.common.io.redis.Redis;
import com.alibaba.alink.common.io.redis.RedisClassLoaderFactory;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.params.io.RedisParams;
import com.alibaba.alink.params.io.RedisStringSinkParams;

import java.io.IOException;
import java.util.Optional;

public class RedisStringOutputFormat extends RichOutputFormat <Row> {

	private final Params params;
	private final String[] colNames;
	private final TypeInformation <?>[] colTypes;
	private final RedisClassLoaderFactory factory;

	private transient int keyColIndic;
	private transient int valColIndic;


	private transient Redis redis;

	public RedisStringOutputFormat(
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

		String keyColName = params.get(RedisStringSinkParams.KEY_COL);
		String valueColName = params.get(RedisStringSinkParams.VALUE_COL);
		
		keyColIndic = TableUtil.findColIndex(dataSchema, keyColName);
		valColIndic = TableUtil.findColIndex(dataSchema, valueColName);

		if (!dataSchema.getFieldTypes()[keyColIndic].equals(AlinkTypes.STRING) ||
			!dataSchema.getFieldTypes()[valColIndic].equals(AlinkTypes.STRING)){
			throw new IllegalArgumentException("RedisStringSinkBatchOp  key value columns require String data");
		}
	}

	@Override
	public void writeRecord(Row record) throws IOException {
		redis.set(
			(String) record.getField(keyColIndic),
			(String) record.getField(valColIndic)
		);
	}

	@Override
	public void close() throws IOException {
		if (redis != null) {
			redis.close();
		}
	}
}
