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

package com.alibaba.alink.operator.common.io.kafka011;

import com.alibaba.alink.operator.common.io.serde.RowToCsvSerialization;
import com.alibaba.alink.operator.common.io.serde.RowToJsonSerialization;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.types.Row;

import java.util.Properties;

public class Kafka011SinkBuilder {
    private String topic;
    private String format;
    private String fieldDelimiter;
    private String[] fieldNames;
    private TypeInformation<?>[] fieldTypes;
    private Properties properties;

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setFieldDelimiter(String fieldDelimiter) {
        this.fieldDelimiter = fieldDelimiter;
    }

    public void setFieldNames(String[] fieldNames) {
        this.fieldNames = fieldNames;
    }

    public void setFieldTypes(TypeInformation<?>[] fieldTypes) {
        this.fieldTypes = fieldTypes;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public Kafka011SinkBuilder() {
    }

    public RichSinkFunction<Row> build() {
        SerializationSchema<Row> serializationSchema;
        if (format.equalsIgnoreCase("csv")) {
            serializationSchema = new RowToCsvSerialization(fieldTypes, fieldDelimiter);
        } else if (format.equalsIgnoreCase("json")) {
            serializationSchema = new RowToJsonSerialization(fieldNames);
        } else {
            throw new IllegalArgumentException("Unknown format " + format);
        }
        return new FlinkKafkaProducer011<Row>(topic, serializationSchema, properties);
    }
}
