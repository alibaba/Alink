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

import com.alibaba.alink.params.io.KafkaSourceParams;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.Properties;

public abstract class BaseKafkaSourceBuilder {
    protected List<String> topic;
    protected String topicPattern;
    protected KafkaSourceParams.StartupMode startupMode;
    protected Properties properties;
    protected long startTimeMs;

    public void setTopic(List<String> topic) {
        this.topic = topic;
    }

    public void setTopicPattern(String topicPattern) {
        this.topicPattern = topicPattern;
    }

    public void setStartupMode(KafkaSourceParams.StartupMode startupMode) {
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
    public abstract RichParallelSourceFunction<Row> build();
}
