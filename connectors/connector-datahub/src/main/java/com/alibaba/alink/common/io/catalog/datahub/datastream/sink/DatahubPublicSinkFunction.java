package com.alibaba.alink.common.io.catalog.datahub.datastream.sink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.io.catalog.datahub.datastream.source.DatahubPublicSourceFunction;
import com.aliyun.datahub.client.model.Field;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.client.model.RecordSchema;
import com.aliyun.datahub.client.model.TupleRecordData;

public class DatahubPublicSinkFunction extends DatahubSinkFunction <Row> {

	public DatahubPublicSinkFunction(
		String endPoint, String projectName, String topicName, String accessKeyId,
		String accessKeySecret, RowTypeInfo rowTypeInfo) {

		super(
			endPoint, projectName, topicName,
			accessKeyId, accessKeySecret, new RowDatahubRecordResolver(rowTypeInfo)
		);
	}

	private static class RowDatahubRecordResolver implements DatahubRecordResolver <Row> {

		private final RowTypeInfo rowTypeInfo;

		private transient RecordSchema recordSchema;

		public RowDatahubRecordResolver(RowTypeInfo rowTypeInfo) {
			this.rowTypeInfo = rowTypeInfo;
		}

		private static RecordSchema rowTypeInfoToRecordSchema(RowTypeInfo rowTypeInfo) {
			String[] names = rowTypeInfo.getFieldNames();
			TypeInformation <?>[] types = rowTypeInfo.getFieldTypes();

			RecordSchema recordSchema = new RecordSchema();

			for (int i = 0; i < types.length; ++i) {
				recordSchema.addField(
					new Field(names[i], DatahubPublicSourceFunction.flinkTypeToDatahubType(types[i]))
				);
			}

			return recordSchema;
		}

		@Override
		public void open() {
			recordSchema = rowTypeInfoToRecordSchema(rowTypeInfo);
		}

		@Override
		public void close() {
			// pass
		}

		@Override
		public RecordEntry getRecordEntry(Row record) {
			TupleRecordData recordData = new TupleRecordData(recordSchema);

			for (int i = 0; i < record.getArity(); ++i) {
				recordData.setField(i, record.getField(i));
			}

			RecordEntry recordEntry = new RecordEntry();

			recordEntry.setRecordData(recordData);

			return recordEntry;
		}
	}
}
