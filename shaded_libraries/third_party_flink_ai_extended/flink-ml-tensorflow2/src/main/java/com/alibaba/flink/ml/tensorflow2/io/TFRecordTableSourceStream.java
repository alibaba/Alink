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

package com.alibaba.flink.ml.tensorflow2.io;

import com.alibaba.flink.ml.operator.util.TypeUtil;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;

/**
 * flink table source: read tensorflow TFRecord format file, output Row.
 */
public class TFRecordTableSourceStream implements StreamTableSource<Row> {

	private static final Logger LOG = LoggerFactory.getLogger(TFRecordTableSourceStream.class);
	private static final TableSchema SCHEMA = TableSchema.builder()
			.field("tfr", TypeInformation.of(byte[].class)).build();

	protected final String[] paths;
	protected final int epochs;

	public TFRecordTableSourceStream(String[] paths, int epochs) {
		this.paths = paths;
		this.epochs = epochs;
	}

	public TFRecordTableSourceStream(File[] files, int epochs) {
		paths = new String[files.length];
		for (int i = 0; i < paths.length; i++) {
			paths[i] = files[i].getAbsolutePath();
		}
		this.epochs = epochs;
	}

	@Override
	public TypeInformation<Row> getReturnType() {
		return new RowTypeInfo(SCHEMA.getFieldTypes(), SCHEMA.getFieldNames());
	}

	@Override
	public TableSchema getTableSchema() {
		return SCHEMA;
	}

	@Override
	public String explainSource() {
		return "TFRecord source " + Arrays.toString(paths);
	}

	@Override
	public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
		return execEnv.createInput(new TFRecordToRowInputFormat(paths, epochs,
				TypeUtil.schemaToRowTypeInfo(SCHEMA))).setParallelism(paths.length).name(explainSource());
	}
}
