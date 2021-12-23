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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.sinks.TableSink;

import org.apache.flink.types.Row;

/**
 * a common flink table sink function.
 */
public abstract class TableDummySinkBase implements TableSink<Row> {
	private final TypeInformation<Row> outType;
	private String[] columnNames;
	private final TypeInformation[] colTypes;

	protected TableDummySinkBase() {
		this(TableSchema.builder().field("dummy", Types.STRING()).build());
	}

	public TableDummySinkBase(TableSchema schema) {
		outType = Types.ROW(schema.getFieldNames(), schema.getFieldTypes());
		columnNames = schema.getFieldNames();
		colTypes = schema.getFieldTypes();
	}


	@Override
	public TypeInformation<Row> getOutputType() {
		return outType;
	}

	@Override
	public String[] getFieldNames() {
		return columnNames;
	}

	@Override
	public TypeInformation<?>[] getFieldTypes() {
		return colTypes;
	}


	TableSchema createSchema(String[] fieldNames, TypeInformation[] fieldTypes) {
		return new TableSchema(fieldNames, fieldTypes);
	}
}
