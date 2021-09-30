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

package com.alibaba.flink.ml.operator.ops.table;

import com.alibaba.flink.ml.operator.ops.sink.DummySink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

/**
 * flink table sink wrap DummySink.
 */
public class TableStreamDummySink extends TableDummySinkBase implements AppendStreamTableSink<Row> {
	public TableStreamDummySink() {
	}

	TableStreamDummySink(TableSchema schema) {
		super(schema);
	}

	@Override
	public TableSink<Row> configure(String[] strings, TypeInformation<?>[] typeInformations) {
		return new TableStreamDummySink(createSchema(strings, typeInformations));
	}

	@Override
	public DataStreamSink<?> consumeDataStream(DataStream<Row> dataStream) {
		return dataStream.addSink(new DummySink<>()).setParallelism(1);
	}
}
