/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.alibaba.alink.operator.common.io.kafka010;

import com.alibaba.alink.operator.common.io.kafka.BaseKafkaSourceBuilder;
import com.alibaba.alink.operator.common.io.kafka.KafkaMessageDeserialization;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.regex.Pattern;

import static com.alibaba.alink.operator.common.io.kafka.KafkaMessageDeserialization.KAFKA_SRC_FIELD_NAMES;
import static com.alibaba.alink.operator.common.io.kafka.KafkaMessageDeserialization.KAFKA_SRC_FIELD_TYPES;

public final class Kafka010SourceBuilder extends BaseKafkaSourceBuilder {
    private static class MessageDeserialization implements KafkaDeserializationSchema<Row> {
        @Override
        public boolean isEndOfStream(Row nextElement) {
            return false;
        }

        @Override
        public Row deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
            return KafkaMessageDeserialization.deserialize(
                record.key(), record.value(), record.topic(), record.partition(), record.offset()
            );
        }

        @Override
        public TypeInformation<Row> getProducedType() {
            return new RowTypeInfo(KAFKA_SRC_FIELD_TYPES, KAFKA_SRC_FIELD_NAMES);
        }
    }

    @Override
    public RichParallelSourceFunction<Row> build() {
        FlinkKafkaConsumer010<Row> consumer;
        if (!StringUtils.isNullOrWhitespaceOnly(topicPattern)) {
            Pattern pattern = Pattern.compile(topicPattern);
            consumer = new FlinkKafkaConsumer010<Row>(pattern, new MessageDeserialization(), properties);
        } else {
            consumer = new FlinkKafkaConsumer010<Row>(topic, new MessageDeserialization(), properties);
        }
        switch (super.startupMode) {
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
