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

import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.operator.common.io.csv.CsvUtil;
import com.alibaba.alink.operator.common.io.kafka011.Kafka011SinkBuilder;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.io.Kafka011SinkParams;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * Data sink for kafka 0.11.x.
 */
@IoOpAnnotation(name = "kafka011", ioType = IOType.SinkStream)
public final class Kafka011SinkStreamOp extends BaseSinkStreamOp<Kafka011SinkStreamOp>
    implements Kafka011SinkParams<Kafka011SinkStreamOp> {


    public Kafka011SinkStreamOp() {
        this(new Params());
    }

    public Kafka011SinkStreamOp(Params params) {
        super(AnnotationUtils.annotatedName(Kafka011SinkStreamOp.class), params);
    }

    @Override
    public Kafka011SinkStreamOp sinkFrom(StreamOperator in) {
        final String dataFormat = getDataFormat();
        final String fieldDelimiter = CsvUtil.unEscape(getFieldDelimiter());

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());

        Kafka011SinkBuilder builder = new Kafka011SinkBuilder();
        builder.setTopic(getTopic());
        builder.setFieldDelimiter(fieldDelimiter);
        builder.setFieldNames(in.getColNames());
        builder.setFieldTypes(in.getColTypes());
        builder.setFormat(dataFormat);
        builder.setProperties(props);

        RichSinkFunction<Row> sink = builder.build();
        in.getDataStream().addSink(sink).name("kafka011");
        return this;
    }
}

