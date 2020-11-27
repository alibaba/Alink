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

package com.alibaba.alink.operator.stream.sink;

import com.alibaba.alink.operator.common.io.kafka.BaseKafkaSinkBuilder;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.io.KafkaSinkParams;
import com.alibaba.alink.params.io.shared.HasDataFormat;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import java.util.Properties;


public abstract class BaseKafkaSinkStreamOp<T extends BaseKafkaSinkStreamOp<T>>
    extends BaseSinkStreamOp<T> {

    public BaseKafkaSinkStreamOp(String nameSrcSnk, Params params) {
        super(nameSrcSnk, params);
    }

    protected abstract BaseKafkaSinkBuilder getKafkaSinkBuilder();

    @Override
    public T sinkFrom(StreamOperator in) {
        String topic = getParams().get(KafkaSinkParams.TOPIC);
        String fieldDelimiter = getParams().get(KafkaSinkParams.FIELD_DELIMITER);
        HasDataFormat.DataFormat dataFormat = getParams().get(KafkaSinkParams.DATA_FORMAT);
        String bootstrapServer = getParams().get(KafkaSinkParams.BOOTSTRAP_SERVERS);
        String properties = getParams().get(KafkaSinkParams.PROPERTIES);

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

        BaseKafkaSinkBuilder builder = getKafkaSinkBuilder();
        builder.setTopic(topic);
        builder.setFieldDelimiter(fieldDelimiter);
        builder.setFieldNames(in.getColNames());
        builder.setFieldTypes(in.getColTypes());
        builder.setFormat(dataFormat);
        builder.setProperties(props);

        RichSinkFunction<Row> sink = builder.build();
        in.getDataStream().addSink(sink).name("kafka");
        return (T) this;
    }
}

