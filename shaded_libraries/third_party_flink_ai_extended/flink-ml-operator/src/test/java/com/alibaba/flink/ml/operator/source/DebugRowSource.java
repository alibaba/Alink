/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.flink.ml.operator.source;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.types.Row;


public class DebugRowSource implements ParallelSourceFunction<Row>, ResultTypeQueryable {
    public static RowTypeInfo typeInfo;
    static {
        TypeInformation[] types = new TypeInformation[5];
        types[0] = BasicTypeInfo.INT_TYPE_INFO;
        types[1] = BasicTypeInfo.LONG_TYPE_INFO;
        types[2] = BasicTypeInfo.FLOAT_TYPE_INFO;
        types[3] = BasicTypeInfo.DOUBLE_TYPE_INFO;
        types[4] = BasicTypeInfo.STRING_TYPE_INFO;
        String[] names = {"a", "b", "c", "d", "e"};
        typeInfo = new RowTypeInfo(types, names);
    }

    public DebugRowSource() {

    }

    @Override
    public void run(SourceContext<Row> ctx) throws Exception {
        for(int i = 0; i < 20; i++){
            String str = String.valueOf(i);
            Row row = new Row(5);
            row.setField(0, Integer.valueOf(str));
            row.setField(1, Long.valueOf(str));
            row.setField(2, Float.valueOf(str));
            row.setField(3, Double.valueOf(str));
            row.setField(4, str);
            ctx.collect(row);
            //Thread.sleep(500);
        }
    }

    @Override
    public void cancel() {

    }

    @Override
    public TypeInformation getProducedType() {
        return typeInfo;
    }
}
