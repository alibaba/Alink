package com.alibaba.alink.common.io.kafka.plugin;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import com.alibaba.alink.common.io.kafka.plugin.KafkaSourceBuilder.StartupMode;
import com.alibaba.alink.params.io.KafkaSinkParams;
import com.alibaba.alink.params.io.KafkaSourceParams;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class KafkaSourceSinkInPluginFactory implements KafkaSourceSinkFactory {

	@Override
	public Tuple2 <RichParallelSourceFunction <Row>, TableSchema> createKafkaSourceFunction(Params params) {
		String topic = params.get(KafkaSourceParams.TOPIC);
		String topicPattern = params.get(KafkaSourceParams.TOPIC_PATTERN);
		StartupMode startupMode = StartupMode.valueOf(
			params.get(KafkaSourceParams.STARTUP_MODE).toString());
		String properties = params.get(KafkaSourceParams.PROPERTIES);

		Preconditions.checkArgument(!StringUtils.isNullOrWhitespaceOnly(topicPattern) ||
			!StringUtils.isNullOrWhitespaceOnly(topic));

		Properties props = new Properties();
		props.setProperty("group.id", params.get(KafkaSourceParams.GROUP_ID));
		props.setProperty("bootstrap.servers", params.get(KafkaSourceParams.BOOTSTRAP_SERVERS));

		if (!StringUtils.isNullOrWhitespaceOnly(properties)) {
			String[] kvPairs = properties.split(",");
			for (String kvPair : kvPairs) {
				int pos = kvPair.indexOf('=');
				Preconditions.checkArgument(pos >= 0, "Invalid properties format, should be \"k1=v1,k2=v2,...\"");
				String key = kvPair.substring(0, pos);
				String value = kvPair.substring(pos + 1);
				props.setProperty(key, value);
			}
		}

		KafkaSourceBuilder builder = new KafkaSourceBuilder();

		if (!StringUtils.isNullOrWhitespaceOnly(topicPattern)) {
			builder.setTopicPattern(topicPattern);
		} else {
			List <String> topics = Arrays.asList(topic.split(","));
			builder.setTopic(topics);
		}
		builder.setProperties(props);
		builder.setStartupMode(startupMode);

		if (startupMode.equals(StartupMode.TIMESTAMP)) {
			String timeStr = params.get(KafkaSourceParams.START_TIME);
			builder.setStartTimeMs(KafkaUtils.parseDateStringToMs(timeStr));
		}

		return Tuple2.of(
			builder.build(),
			new TableSchema(
				KafkaMessageDeserialization.KAFKA_SRC_FIELD_NAMES,
				KafkaMessageDeserialization.KAFKA_SRC_FIELD_TYPES
			)
		);
	}

	@Override
	public RichSinkFunction <Row> createKafkaSinkFunction(Params params, TableSchema schema) {
		String topic = params.get(KafkaSinkParams.TOPIC);
		String fieldDelimiter = params.get(KafkaSinkParams.FIELD_DELIMITER);
		String dataFormat = params.get(KafkaSinkParams.DATA_FORMAT).toString();
		String bootstrapServer = params.get(KafkaSinkParams.BOOTSTRAP_SERVERS);
		String properties = params.get(KafkaSinkParams.PROPERTIES);

		Properties props = new Properties();
		props.setProperty("bootstrap.servers", bootstrapServer);

		if (!StringUtils.isNullOrWhitespaceOnly(properties)) {
			String[] kvPairs = properties.split(",");
			for (String kvPair : kvPairs) {
				int pos = kvPair.indexOf('=');
				Preconditions.checkArgument(pos >= 0, "Invalid properties format, should be \"k1=v1,k2=v2,...\"");
				String key = kvPair.substring(0, pos);
				String value = kvPair.substring(pos + 1);
				props.setProperty(key, value);
			}
		}

		KafkaSinkBuilder builder = new KafkaSinkBuilder();
		builder.setTopic(topic);
		builder.setFieldDelimiter(fieldDelimiter);
		builder.setFieldNames(schema.getFieldNames());
		builder.setFieldTypes(schema.getFieldTypes());
		builder.setFormat(dataFormat);
		builder.setProperties(props);

		return builder.build();
	}
}
