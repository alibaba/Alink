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

import com.alibaba.alink.common.utils.JsonConverter;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.types.Row;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;

public class RowToJsonSerialization implements SerializationSchema<Row> {
    private String[] columnNames;

    public RowToJsonSerialization(String[] columnNames) {
        this.columnNames = columnNames;
    }

    @Override
    public byte[] serialize(Row element) {
        HashMap<String, Object> map = new HashMap<>();
        for (int i = 0; i < columnNames.length; i++) {
            Object obj = element.getField(i);
            if (obj != null) {
                map.put(columnNames[i], obj);
            }
        }
        String str = JsonConverter.toJson(map);
        return str.getBytes(StandardCharsets.UTF_8);
    }

}
