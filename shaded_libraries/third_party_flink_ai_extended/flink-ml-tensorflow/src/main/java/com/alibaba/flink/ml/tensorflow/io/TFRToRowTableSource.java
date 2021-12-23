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

package com.alibaba.flink.ml.tensorflow.io;


import com.alibaba.flink.ml.operator.util.TypeUtil;
import com.google.common.base.Preconditions;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

import java.io.File;
import java.util.Arrays;

/**
 * TFRToRowInputFormat corresponds to flink table source function.
 */
public class TFRToRowTableSource implements StreamTableSource<Row> {

	private final String[] paths;
	private final int epochs;
	private final RowTypeInfo outRowType;
	private final String[] outColAliases;
	private final TFRExtractRowHelper.ScalarConverter[] converters;

	public TFRToRowTableSource(String[] paths, int epochs, RowTypeInfo outRowType, String[] outColAliases,
			TFRExtractRowHelper.ScalarConverter[] converters) {
		Preconditions.checkArgument(outRowType.getArity() == outColAliases.length);
		this.paths = paths;
		this.epochs = epochs;
		this.outRowType = outRowType;
		this.outColAliases = outColAliases;
		this.converters = converters;
	}

	public TFRToRowTableSource(String[] paths, int epochs, RowTypeInfo outRowType,
			TFRExtractRowHelper.ScalarConverter[] converters) {
		this(paths, epochs, outRowType, outRowType.getFieldNames(), converters);
	}

	public TFRToRowTableSource(File[] files, int epochs, RowTypeInfo outRowType,
			TFRExtractRowHelper.ScalarConverter[] converters) {
		this(files, epochs, outRowType, outRowType.getFieldNames(), converters);
	}

	public TFRToRowTableSource(File[] files, int epochs, RowTypeInfo outRowType, String[] outColAliases,
			TFRExtractRowHelper.ScalarConverter[] converters) {
		this(Arrays.stream(files).map(f -> f.getAbsolutePath()).toArray(String[]::new), epochs, outRowType,
				outColAliases, converters);
	}

	@Override
	public TypeInformation<Row> getReturnType() {
		return new RowTypeInfo(outRowType.getFieldTypes(), outColAliases);
	}

	@Override
	public TableSchema getTableSchema() {
		return TypeUtil.rowTypeInfoToSchema((outRowType));
	}

	@Override
	public String explainSource() {
		return String.format("TFRecord source %s to %s", Arrays.toString(paths), outRowType.toString());
	}

	@Override
	public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
		return execEnv.createInput(new TFRToRowInputFormat(paths, epochs, outRowType, outColAliases, converters))
				.setParallelism(paths.length).name(explainSource());
	}
}
