package com.alibaba.alink.common.io.catalog;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.DataStreamConversionUtil;
import com.alibaba.alink.operator.batch.BatchOperator;

public abstract class InputOutputFormatCatalog extends BaseCatalog {
	public InputOutputFormatCatalog(Params params) {
		super(params);
	}

	public static final ParamInfo<Integer> PARALLELISM = ParamInfoFactory
		.createParamInfo("parallelism", Integer.class)
		.build();

	@Override
	public Table sourceStream(ObjectPath objectPath, Params params, Long sessionId) {
		TableSchema schema;
		InputFormat <Row, InputSplit> inputFormat;

		int parallelism = MLEnvironmentFactory
			.get(sessionId)
			.getStreamExecutionEnvironment()
			.getParallelism();

		try {
			schema = getTable(objectPath).getSchema();
			inputFormat = createInputFormat(
				objectPath, schema, getParams().merge(params).set(PARALLELISM, parallelism)
			);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		return DataStreamConversionUtil.toTable(
			sessionId,
			MLEnvironmentFactory
				.get(sessionId)
				.getStreamExecutionEnvironment()
				.createInput(inputFormat, new RowTypeInfo(schema.getFieldTypes())),
			schema
		);
	}

	@Override
	public void sinkStream(ObjectPath objectPath, Table in, Params params, Long sessionId) {
		OutputFormat <Row> outputFormat = createOutputFormat(objectPath, in.getSchema(), getParams().merge(params));

		MLEnvironmentFactory.get(sessionId)
			.getStreamTableEnvironment()
			.toAppendStream(in, new RowTypeInfo(in.getSchema().getFieldTypes()))
			.writeUsingOutputFormat(outputFormat);
	}

	@Override
	public Table sourceBatch(ObjectPath objectPath, Params params, Long sessionId) {

		TableSchema schema;
		InputFormat <Row, InputSplit> inputFormat;

		int parallelism = MLEnvironmentFactory
			.get(sessionId)
			.getStreamExecutionEnvironment()
			.getParallelism();

		try {
			schema = getTable(objectPath).getSchema();
			inputFormat = createInputFormat(objectPath, schema, getParams().merge(params).set(PARALLELISM, parallelism));
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}

		return DataSetConversionUtil.toTable(
			sessionId,
			MLEnvironmentFactory
				.get(sessionId)
				.getExecutionEnvironment()
				.createInput(inputFormat, new RowTypeInfo(schema.getFieldTypes())),
			schema
		);
	}

	@Override
	public void sinkBatch(ObjectPath objectPath, Table in, Params params, Long sessionId) {
		OutputFormat <Row> outputFormat = createOutputFormat(objectPath, in.getSchema(), getParams().merge(params));
		BatchOperator.fromTable(in).setMLEnvironmentId(sessionId).getDataSet().output(outputFormat);
	}

	protected abstract RichInputFormat <Row, InputSplit> createInputFormat(
		ObjectPath objectPath, TableSchema schema, Params params) throws Exception;

	protected abstract OutputFormat <Row> createOutputFormat(
		ObjectPath objectPath, TableSchema schema, Params params);
}
