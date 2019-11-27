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

package com.alibaba.alink.operator.common.io.kafka;

import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.StringUtils;

import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

public class Kafka011SourceBuilder {
    private List<String> topic;
    private String topicPattern;
    private String startupMode;
    private RowTypeInfo baseRowTypeInfo;
    private Properties properties;
    private long startTimeMs;

    public void setTopic(List<String> topic) {
        this.topic = topic;
    }

    public void setTopicPattern(String topicPattern) {
        this.topicPattern = topicPattern;
    }

    public void setStartupMode(String startupMode) {
        this.startupMode = startupMode;
    }

    public void setBaseRowTypeInfo(RowTypeInfo baseRowTypeInfo) {
        this.baseRowTypeInfo = baseRowTypeInfo;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public void setStartTimeMs(long startTimeMs) {
        this.startTimeMs = startTimeMs;
    }

    public Kafka011SourceBuilder() {
    }

    public RichParallelSourceFunction<Row> build() {
        FlinkKafkaConsumerBase<Row> consumer;
        KafkaMessageDeserialization kafkaMessageDeserialization = new KafkaMessageDeserialization(baseRowTypeInfo);
        Pattern pattern;
        if (!StringUtils.isNullOrWhitespaceOnly(topicPattern)) {
            pattern = Pattern.compile(topicPattern);
            consumer = new FlinkKafkaConsumer011(pattern, kafkaMessageDeserialization, properties);
        } else {
            consumer = new FlinkKafkaConsumer011(topic, kafkaMessageDeserialization, properties);
        }
        if (startupMode.equalsIgnoreCase("TIMESTAMP")) {
            ((FlinkKafkaConsumer011) consumer).setStartFromTimestamp(startTimeMs);
        } else if (startupMode.equalsIgnoreCase("EARLIEST")) {
            consumer.setStartFromEarliest();
        } else if (startupMode.equalsIgnoreCase("LATEST")) {
            consumer.setStartFromLatest();
        } else if (startupMode.equalsIgnoreCase("GROUP_OFFSETS")) {
            consumer.setStartFromGroupOffsets();
        } else {
            throw new IllegalArgumentException("Unknown startup model: " + startupMode);
        }
        return consumer;
    }
}
