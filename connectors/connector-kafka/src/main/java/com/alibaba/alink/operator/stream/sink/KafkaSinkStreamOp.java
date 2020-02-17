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
import com.alibaba.alink.operator.common.io.kafka.BaseKafkaSinkBuilder;
import com.alibaba.alink.operator.common.io.kafka.KafkaSinkBuilder;
import com.alibaba.alink.params.io.KafkaSinkParams;
import org.apache.flink.ml.api.misc.param.Params;

/**
 * Data sink for kafka 1.x and above.
 */
@IoOpAnnotation(name = "kafka", ioType = IOType.SinkStream)
public final class KafkaSinkStreamOp extends BaseKafkaSinkStreamOp<KafkaSinkStreamOp>
    implements KafkaSinkParams<KafkaSinkStreamOp> {


    public KafkaSinkStreamOp() {
        this(new Params());
    }

    public KafkaSinkStreamOp(Params params) {
        super(AnnotationUtils.annotatedName(KafkaSinkStreamOp.class), params);
    }

    @Override
    protected BaseKafkaSinkBuilder getKafkaSinkBuilder() {
        return new KafkaSinkBuilder();
    }
}