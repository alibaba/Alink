package com.alibaba.alink.common.io.catalog;

import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.utils.DataStreamConversionUtil;

public abstract class SourceSinkFunctionCatalog extends BaseCatalog {
	public SourceSinkFunctionCatalog(Params params) {
		super(params);
	}

	@Override
	public Table sourceStream(ObjectPath objectPath, Params params, Long sessionId) {
		TableSchema schema;
		RichParallelSourceFunction <Row> sourceFunction;

		try {
			schema = getTable(objectPath).getSchema();
			sourceFunction = createSourceFunction(
				objectPath, schema, getParams().merge(params)
			);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		return DataStreamConversionUtil.toTable(
			sessionId,
			MLEnvironmentFactory
				.get(sessionId)
				.getStreamExecutionEnvironment()
				.addSource(sourceFunction, new RowTypeInfo(schema.getFieldTypes())),
			schema
		);
	}

	@Override
	public void sinkStream(ObjectPath objectPath, Table in, Params params, Long sessionId) {
		RichSinkFunction <Row> sinkFunction = createSinkFunction(objectPath, in.getSchema(),
			getParams().merge(params));

		MLEnvironmentFactory.get(sessionId)
			.getStreamTableEnvironment()
			.toAppendStream(in, new RowTypeInfo(in.getSchema().getFieldTypes()))
			.addSink(sinkFunction);
	}

	@Override
	public Table sourceBatch(ObjectPath objectPath, Params params, Long sessionId) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void sinkBatch(ObjectPath objectPath, Table in, Params params, Long sessionId) {
		throw new UnsupportedOperationException();
	}

	protected abstract RichSinkFunction <Row> createSinkFunction(
		ObjectPath objectPath, TableSchema schema, Params params);

	protected abstract RichParallelSourceFunction <Row> createSourceFunction(
		ObjectPath objectPath, TableSchema schema, Params params) throws Exception;
}
