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

package com.alibaba.alink.operator.common.io.serde;

import com.alibaba.alink.operator.common.io.csv.CsvFormatter;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;

import java.nio.charset.StandardCharsets;

public class RowToCsvSerialization implements SerializationSchema<Row> {
    private String fieldDelim;
    private TypeInformation<?>[] typeInfos;
    private CsvFormatter formatter;

    public RowToCsvSerialization(TypeInformation<?>[] typeInfos, String fieldDelim) {
        this.fieldDelim = fieldDelim;
        this.typeInfos = typeInfos;
    }

    @Override
    public byte[] serialize(Row element) {
        if (this.formatter == null) {
            this.formatter = new CsvFormatter(typeInfos, fieldDelim, '\"');
        }
        return this.formatter.format(element).getBytes(StandardCharsets.UTF_8);
    }
}
