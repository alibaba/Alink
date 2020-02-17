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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class KafkaMessageDeserialization {

    public static final String[] KAFKA_SRC_FIELD_NAMES =
        new String[]{"message_key", "message", "topic", "topic_partition", "partition_offset"};

    public static final TypeInformation[] KAFKA_SRC_FIELD_TYPES = new TypeInformation[]{Types.STRING,
        Types.STRING, Types.STRING, Types.INT, Types.LONG};

    public static Row deserialize(byte[] messageKey, byte[] message, String topic, int partition, long offset) throws IOException {
        Row row = new Row(5);
        row.setField(0, messageKey != null ? new String(messageKey, StandardCharsets.UTF_8) : null);
        row.setField(1, message != null ? new String(message, StandardCharsets.UTF_8) : null);
        row.setField(2, topic);
        row.setField(3, partition);
        row.setField(4, offset);
        return row;
    }
}
