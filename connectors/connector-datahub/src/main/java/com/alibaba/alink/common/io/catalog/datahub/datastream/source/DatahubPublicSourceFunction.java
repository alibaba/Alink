package com.alibaba.alink.common.io.catalog.datahub.datastream.source;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.AlinkTypes;
import com.alibaba.alink.common.io.catalog.datahub.datastream.util.DatahubClientProvider;
import com.aliyun.datahub.client.model.Field;
import com.aliyun.datahub.client.model.FieldType;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.client.model.RecordSchema;
import com.aliyun.datahub.client.model.TupleRecordData;

import java.util.List;

public class DatahubPublicSourceFunction extends RichParallelSourceFunction <Row> implements
	ResultTypeQueryable <Row>,
	CheckpointedFunction {

	private final DatahubSourceFunction internal;
	private final RowTypeInfo schema;

	public DatahubPublicSourceFunction(
		String endpoint,
		String projectName,
		String topicName,
		String accessId,
		String accessKey,
		long startInMs,
		long stopInMs) throws Exception {

		internal = new DatahubSourceFunction(
			endpoint,
			projectName,
			topicName,
			accessId,
			accessKey,
			startInMs,
			stopInMs
		);

		schema = getSchemaFromClient(
			createProvider(endpoint, accessId, accessKey, null), projectName, topicName
		);
	}

	public DatahubPublicSourceFunction(
		String endpoint,
		String projectName,
		String topicName,
		String accessId,
		String accessKey,
		long startInMs,
		long stopInMs,
		long maxRetryTimes,
		long retryIntervalMs,
		int batchReadSize) throws Exception {

		internal = new DatahubSourceFunction(
			endpoint,
			projectName,
			topicName,
			accessId,
			accessKey,
			startInMs,
			stopInMs,
			maxRetryTimes,
			retryIntervalMs,
			batchReadSize
		);

		schema = getSchemaFromClient(
			createProvider(endpoint, accessId, accessKey, null), projectName, topicName
		);
	}

	public DatahubPublicSourceFunction(
		String endpoint,
		String projectName,
		String topicName,
		Configuration properties,
		long startInMs,
		long stopInMs,
		long maxRetryTimes,
		long retryIntervalMs,
		int batchReadSize) throws Exception {

		internal = new DatahubSourceFunction(
			endpoint,
			projectName,
			topicName,
			properties,
			startInMs,
			stopInMs,
			maxRetryTimes,
			retryIntervalMs,
			batchReadSize
		);

		schema = getSchemaFromClient(
			createProvider(endpoint, null, null, properties),
			projectName, topicName
		);
	}

	@Override
	public void setRuntimeContext(RuntimeContext t) {
		internal.setRuntimeContext(t);
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		internal.open(parameters);
	}

	@Override
	public void close() throws Exception {
		internal.close();
	}

	@Override
	public TypeInformation <Row> getProducedType() {
		return schema;
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {

		internal.snapshotState(context);
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		internal.initializeState(context);
	}

	@Override
	public void run(SourceContext <Row> ctx) throws Exception {
		internal.run(new SourceContext <List <RecordEntry>>() {
			@Override
			public void collect(List <RecordEntry> element) {
				for (RecordEntry recordEntry : element) {
					ctx.collect(recordEntryToRow(recordEntry));
				}
			}

			@Override
			public void collectWithTimestamp(List <RecordEntry> element, long timestamp) {
				for (RecordEntry recordEntry : element) {
					ctx.collectWithTimestamp(recordEntryToRow(recordEntry), timestamp);
				}
			}

			@Override
			public void emitWatermark(Watermark mark) {
				ctx.emitWatermark(mark);
			}

			@Override
			public void markAsTemporarilyIdle() {
				ctx.markAsTemporarilyIdle();
			}

			@Override
			public Object getCheckpointLock() {
				return ctx.getCheckpointLock();
			}

			@Override
			public void close() {
				ctx.close();
			}
		});
	}

	@Override
	public void cancel() {
		internal.cancel();
	}

	private DatahubClientProvider createProvider(
		String endpoint,
		String accessId,
		String accessKey,
		Configuration properties) {

		if (null != accessId && null != accessKey && !accessId.isEmpty() && !accessKey.isEmpty()) {
			return new DatahubClientProvider(endpoint, accessId, accessKey);
		} else {
			return new DatahubClientProvider(endpoint, properties);
		}
	}

	public static TypeInformation <?> datahubTypeToFlinkType(FieldType fieldType) {

		switch (fieldType) {
			case STRING:
				return AlinkTypes.STRING;
			case BIGINT:
				return AlinkTypes.LONG;
			case DOUBLE:
				return AlinkTypes.DOUBLE;
			case BOOLEAN:
				return AlinkTypes.BOOLEAN;
			case DECIMAL:
				return AlinkTypes.BIG_DEC;
			case TIMESTAMP:
				return AlinkTypes.SQL_TIMESTAMP;
			default:
				throw new IllegalArgumentException("Not supported types. " + fieldType);
		}
	}

	public static FieldType flinkTypeToDatahubType(TypeInformation<?> type) {
		if (type.equals(AlinkTypes.STRING)) {
			return FieldType.STRING;
		} else if (type.equals(AlinkTypes.DOUBLE)) {
			return FieldType.DOUBLE;
		} else if (type.equals(AlinkTypes.BOOLEAN)) {
			return FieldType.BOOLEAN;
		} else if (type.equals(AlinkTypes.BIG_INT)) {
			return FieldType.DECIMAL;
		} else if (type.equals(AlinkTypes.LONG)) {
			return FieldType.BIGINT;
		} else if (type.equals(AlinkTypes.SQL_TIMESTAMP)) {
			return FieldType.TIMESTAMP;
		} else {
			throw new IllegalArgumentException("Unsupported types. " + type);
		}
	}

	public static RowTypeInfo getSchemaFromClient(DatahubClientProvider clientProvider, String project,
												   String topicName) {
		RecordSchema recordSchema = clientProvider.getClient().getTopic(project, topicName).getRecordSchema();

		List <Field> fields = recordSchema.getFields();

		TypeInformation <?>[] types = new TypeInformation <?>[fields.size()];

		for (int i = 0; i < fields.size(); ++i) {
			types[i] = datahubTypeToFlinkType(fields.get(i).getType());
		}

		return new RowTypeInfo(types);
	}

	private static Row recordEntryToRow(RecordEntry recordEntry) {
		TupleRecordData tupleRecordData = (TupleRecordData) recordEntry.getRecordData();

		Row ret = new Row(tupleRecordData.getRecordSchema().getFields().size());

		for (int i = 0; i < tupleRecordData.getRecordSchema().getFields().size(); ++i) {
			ret.setField(i, ((TupleRecordData) recordEntry.getRecordData()).getField(i));
		}

		return ret;
	}

}
