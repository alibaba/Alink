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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.functions.source.InputFormatSourceFunction;
import org.apache.flink.types.Row;

/**
 * TFRToRowInputFormat corresponds to flink source function.
 */
public class TFRToRowSourceFunc extends InputFormatSourceFunction<Row> implements ResultTypeQueryable {

	private final RowTypeInfo outRowType;

	public TFRToRowSourceFunc(String[] paths, int epochs, RowTypeInfo outRowType,
			TFRExtractRowHelper.ScalarConverter[] converters) {
		super(new TFRToRowInputFormat(paths, epochs, outRowType, converters),
				outRowType);
		this.outRowType = outRowType;
	}

	@Override
	public TypeInformation getProducedType() {
		return outRowType;
	}
}
