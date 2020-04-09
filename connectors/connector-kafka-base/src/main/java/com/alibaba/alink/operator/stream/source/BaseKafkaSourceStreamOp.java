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

package com.alibaba.alink.operator.stream.source;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.utils.DataStreamConversionUtil;
import com.alibaba.alink.operator.common.io.kafka.BaseKafkaSourceBuilder;
import com.alibaba.alink.operator.common.io.kafka.KafkaMessageDeserialization;
import com.alibaba.alink.operator.common.io.kafka.KafkaUtils;
import com.alibaba.alink.params.io.KafkaSourceParams;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;


public abstract class BaseKafkaSourceStreamOp<T extends BaseKafkaSourceStreamOp<T>>
    extends BaseSourceStreamOp<T> {


    public BaseKafkaSourceStreamOp(String nameSrcSnk, Params params) {
        super(nameSrcSnk, params);
    }

    protected abstract BaseKafkaSourceBuilder getKafkaSourceBuilder();

    @Override
    protected Table initializeDataSource() {
        String topic = getParams().get(KafkaSourceParams.TOPIC);
        String topicPattern = getParams().get(KafkaSourceParams.TOPIC_PATTERN);
        KafkaSourceParams.StartupMode startupMode = getParams().get(KafkaSourceParams.STARTUP_MODE);
        String properties = getParams().get(KafkaSourceParams.PROPERTIES);

        Preconditions.checkArgument(!StringUtils.isNullOrWhitespaceOnly(topicPattern) ||
            !StringUtils.isNullOrWhitespaceOnly(topic));

        Properties props = new Properties();
        props.setProperty("group.id", getParams().get(KafkaSourceParams.GROUP_ID));
        props.setProperty("bootstrap.servers", getParams().get(KafkaSourceParams.BOOTSTRAP_SERVERS));

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

        BaseKafkaSourceBuilder builder = getKafkaSourceBuilder();

        if (!StringUtils.isNullOrWhitespaceOnly(topicPattern)) {
            builder.setTopicPattern(topicPattern);
        } else {
            List<String> topics = Arrays.asList(topic.split(","));
            builder.setTopic(topics);
        }
        builder.setProperties(props);
        builder.setStartupMode(startupMode);

        if (startupMode.equals(KafkaSourceParams.StartupMode.TIMESTAMP)) {
            String timeStr = getParams().get(KafkaSourceParams.START_TIME);
            builder.setStartTimeMs(KafkaUtils.parseDateStringToMs(timeStr));
        }

        StreamExecutionEnvironment env = MLEnvironmentFactory.get(getMLEnvironmentId()).getStreamExecutionEnvironment();
        DataStream<Row> data = env
            .addSource(builder.build())
            .name("kafka");
        return DataStreamConversionUtil.toTable(getMLEnvironmentId(), data,
            KafkaMessageDeserialization.KAFKA_SRC_FIELD_NAMES,
            KafkaMessageDeserialization.KAFKA_SRC_FIELD_TYPES);
    }
}

