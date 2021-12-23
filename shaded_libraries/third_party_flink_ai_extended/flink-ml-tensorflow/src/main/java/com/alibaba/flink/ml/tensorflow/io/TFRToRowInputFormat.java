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

import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.types.Row;

import java.io.IOException;

/**
 * flink input format: input tensorflow TFRecord format file, output flink table row.
 * a TFRecord type record contain  serialized Example.
 * a feature of Example corresponds to a column of row.
 */
public class TFRToRowInputFormat extends RichInputFormat<Row, TFRecordInputSplit> implements ResultTypeQueryable<Row> {

	private final TFRecordInputFormat inputFormat;
	private final RowTypeInfo outRowType;
	private final String[] outColAliases;
	private final TFRExtractRowHelper extractRowHelper;

	public TFRToRowInputFormat(String[] paths, int epochs, RowTypeInfo outRowType,
			TFRExtractRowHelper.ScalarConverter[] converters) {
		this(paths, epochs, outRowType, outRowType.getFieldNames(), converters);
	}

	public TFRToRowInputFormat(String[] paths, int epochs, RowTypeInfo outRowType, String[] outColAliases,
			TFRExtractRowHelper.ScalarConverter[] converters) {
		inputFormat = new TFRecordInputFormat(paths, epochs);
		this.outRowType = outRowType;
		this.outColAliases = outColAliases;
		extractRowHelper = new TFRExtractRowHelper(outRowType, converters);
	}

	@Override
	public void configure(Configuration parameters) {
	}

	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
		return inputFormat.getStatistics(cachedStatistics);
	}

	@Override
	public TFRecordInputSplit[] createInputSplits(int minNumSplits) throws IOException {
		return inputFormat.createInputSplits(minNumSplits);
	}

	@Override
	public InputSplitAssigner getInputSplitAssigner(TFRecordInputSplit[] inputSplits) {
		return inputFormat.getInputSplitAssigner(inputSplits);
	}

	@Override
	public void open(TFRecordInputSplit split) throws IOException {
		inputFormat.open(split);
	}

	@Override
	public boolean reachedEnd() throws IOException {
		return inputFormat.reachedEnd();
	}

	@Override
	public void close() throws IOException {
		inputFormat.close();
	}

	@Override
	public TypeInformation<Row> getProducedType() {
		return outRowType;
	}

	@Override
	public Row nextRecord(Row reuse) throws IOException {
		byte[] bytes = inputFormat.nextRecord(null);
		if (bytes != null) {
			return extractRowHelper.extract(bytes);
		}
		return null;
	}
}
