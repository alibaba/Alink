package com.alibaba.alink.common.io.kafka.plugin;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.io.serde.RowToCsvSerialization;
import com.alibaba.alink.operator.common.io.serde.RowToJsonSerialization;

import java.util.Properties;

public final class KafkaSinkBuilder {
	protected String topic;
	protected OutputFormat format;
	protected String fieldDelimiter;
	protected String[] fieldNames;
	protected TypeInformation <?>[] fieldTypes;
	protected Properties properties;

	protected enum OutputFormat {
		CSV,
		JSON
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public void setFieldDelimiter(String fieldDelimiter) {
		this.fieldDelimiter = fieldDelimiter;
	}

	public void setFieldNames(String[] fieldNames) {
		this.fieldNames = fieldNames;
	}

	public void setFieldTypes(TypeInformation <?>[] fieldTypes) {
		this.fieldTypes = fieldTypes;
	}

	public void setFormat(String format) {
		this.format = OutputFormat.valueOf(format.toUpperCase());
	}

	public void setProperties(Properties properties) {
		this.properties = properties;
	}

	protected SerializationSchema <Row> getSerializationSchema() {
		switch (this.format) {
			case CSV:
				return new RowToCsvSerialization(fieldTypes, fieldDelimiter);
			case JSON:
				return new RowToJsonSerialization(fieldNames);
			default:
				throw new IllegalArgumentException("Invalid format: " + format);
		}
	}

	/**
	 * Construct the {@link RichSinkFunction} for specific version of Kafka.
	 */
	public RichSinkFunction <Row> build() {
		SerializationSchema <Row> serializationSchema = getSerializationSchema();
		return new FlinkKafkaProducer <>(topic, serializationSchema, properties);
	}
}
