package com.alibaba.alink.common.io.kafka.plugin;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;

import java.nio.charset.StandardCharsets;

public class KafkaMessageDeserialization {

	public static final String[] KAFKA_SRC_FIELD_NAMES =
		new String[] {"message_key", "message", "topic", "topic_partition", "partition_offset"};

	public static final TypeInformation <?>[] KAFKA_SRC_FIELD_TYPES = new TypeInformation[] {Types.STRING,
		Types.STRING, Types.STRING, Types.INT, Types.LONG};

	public static Row deserialize(byte[] messageKey, byte[] message, String topic, int partition, long offset) {
		Row row = new Row(5);
		row.setField(0, messageKey != null ? new String(messageKey, StandardCharsets.UTF_8) : null);
		row.setField(1, message != null ? new String(message, StandardCharsets.UTF_8) : null);
		row.setField(2, topic);
		row.setField(3, partition);
		row.setField(4, offset);
		return row;
	}
}
