package com.alibaba.alink.common.io.kafka;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.StringUtils;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

public class KafkaSourceBuilder {
	protected List <String> topic;
	protected String topicPattern;
	protected StartupMode startupMode;
	protected Properties properties;
	protected long startTimeMs;

	public enum StartupMode {
		TIMESTAMP,
		EARLIEST,
		LATEST,
		GROUP_OFFSETS
	}

	private static class MessageDeserialization implements KafkaDeserializationSchema <Row> {
		@Override
		public boolean isEndOfStream(Row nextElement) {
			return false;
		}

		@Override
		public Row deserialize(ConsumerRecord <byte[], byte[]> record) throws Exception {
			return KafkaMessageDeserialization.deserialize(
				record.key(), record.value(), record.topic(), record.partition(), record.offset()
			);
		}

		@Override
		public TypeInformation <Row> getProducedType() {
			return new RowTypeInfo(
				KafkaMessageDeserialization.KAFKA_SRC_FIELD_TYPES,
				KafkaMessageDeserialization.KAFKA_SRC_FIELD_NAMES
			);
		}
	}

	public void setTopic(List <String> topic) {
		this.topic = topic;
	}

	public void setTopicPattern(String topicPattern) {
		this.topicPattern = topicPattern;
	}

	public void setStartupMode(StartupMode startupMode) {
		this.startupMode = startupMode;
	}

	public void setProperties(Properties properties) {
		this.properties = properties;
	}

	public void setStartTimeMs(long startTimeMs) {
		this.startTimeMs = startTimeMs;
	}

	/**
	 * Construct the {@link RichParallelSourceFunction} for specific version of Kafka.
	 */
	public RichParallelSourceFunction <Row> build() {
		FlinkKafkaConsumer <Row> consumer;

		if (!StringUtils.isNullOrWhitespaceOnly(topicPattern)) {
			Pattern pattern = Pattern.compile(topicPattern);
			consumer = new FlinkKafkaConsumer <>(pattern, new MessageDeserialization(), properties);
		} else {
			consumer = new FlinkKafkaConsumer <>(topic, new MessageDeserialization(), properties);
		}

		switch (startupMode) {
			case LATEST: {
				consumer.setStartFromLatest();
				break;
			}
			case EARLIEST: {
				consumer.setStartFromEarliest();
				break;
			}
			case GROUP_OFFSETS: {
				consumer.setStartFromGroupOffsets();
				break;
			}
			case TIMESTAMP: {
				consumer.setStartFromTimestamp(startTimeMs);
				break;
			}
			default: {
				throw new IllegalArgumentException("invalid startupMode.");
			}
		}

		return consumer;
	}
}
