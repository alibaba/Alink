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

package com.alibaba.flink.ml.operator.coding;

import com.alibaba.flink.ml.cluster.node.MLContext;
import com.alibaba.flink.ml.coding.Coding;
import com.alibaba.flink.ml.coding.CodingException;
import com.alibaba.flink.ml.operator.util.DataTypes;
import com.alibaba.flink.ml.util.MLConstants;
import org.apache.flink.types.Row;

/**
 * implement coding interface.
 * convert row object to csv format and csv format record to row object.
 * csv format(if delim is "," column1,column2,column3)
 */
public class RowCSVCoding implements Coding<Row> {
	public static String DELIM_CONFIG = MLConstants.SYS_PREFIX + "delim";
	public static String ENCODE_TYPES = MLConstants.SYS_PREFIX + "csv_encode_types";
	public static String DECODE_TYPES = MLConstants.SYS_PREFIX + "csv_decode_types";
	public static String TYPES_SPLIT_CONFIG = ",";


	private DataTypes[] encodeTypes;
	private DataTypes[] decodeTypes;

	private String delim;

	public RowCSVCoding(MLContext mlContext) {
		String encodeTypesStr = mlContext.getProperties().getOrDefault(ENCODE_TYPES, DataTypes.STRING.name());
		String[] encodeTypeStrArray = encodeTypesStr.split(TYPES_SPLIT_CONFIG);
		encodeTypes = new DataTypes[encodeTypeStrArray.length];
		for (int i = 0; i < encodeTypeStrArray.length; i++) {
			encodeTypes[i] = DataTypes.valueOf(encodeTypeStrArray[i]);
		}
		String decodeTypesStr = mlContext.getProperties().getOrDefault(DECODE_TYPES, DataTypes.STRING.name());
		String[] decodeTypeStrArray = decodeTypesStr.split(TYPES_SPLIT_CONFIG);
		decodeTypes = new DataTypes[decodeTypeStrArray.length];
		for (int i = 0; i < decodeTypeStrArray.length; i++) {
			decodeTypes[i] = DataTypes.valueOf(decodeTypeStrArray[i]);
		}
		delim = mlContext.getProperties().getOrDefault(DELIM_CONFIG, TYPES_SPLIT_CONFIG);
	}

	/**
	 * convert byte[](csv format) to flink table row object.
	 * @param bytes csv format record byte array.
	 * @return table row object.
	 * @throws CodingException
	 */
	@Override
	public Row decode(byte[] bytes) throws CodingException {
		String str = new String(bytes);
		String[] tmp = str.split(delim);
		if (tmp.length != decodeTypes.length) {
			throw new CodingException("record num != names num");
		}
		Row row = new Row(decodeTypes.length);
		for (int i = 0; i < decodeTypes.length; i++) {
			switch (decodeTypes[i]) {
				case INT_32: {
					row.setField(i, Integer.valueOf(tmp[i]));
					break;
				}
				case INT_64: {
					row.setField(i, Long.valueOf(tmp[i]));
					break;
				}
				case FLOAT_32: {
					row.setField(i, Float.valueOf(tmp[i]));
					break;
				}
				case FLOAT_64: {
					row.setField(i, Double.valueOf(tmp[i]));
					break;
				}
				case STRING: {
					row.setField(i, tmp[i]);
					break;
				}
				default:
					throw new CodingException("RowCSVCoding not support:" + decodeTypes[i].name());
			}
		}
		return row;
	}

	/**
	 * convert flink table row object to byte[](csv format).
	 * @param object table row object.
	 * @return csv format record string.
	 * @throws CodingException
	 */
	@Override
	public byte[] encode(Row object) throws CodingException {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < encodeTypes.length; i++) {
			switch (encodeTypes[i]) {
				case INT_32: {
					break;
				}
				case INT_64: {
					break;
				}
				case FLOAT_32: {
					break;
				}
				case FLOAT_64: {
					break;
				}
				case STRING: {
					break;
				}
				default:
					throw new CodingException("RowCSVCoding not support:" + encodeTypes[i].name());
			}
			sb.append(object.getField(i));
			if (i != encodeTypes.length - 1) {
				sb.append(delim);
			}
		}
		return sb.toString().getBytes();
	}

	public String getDelim() {
		return delim;
	}

	public void setDelim(String delim) {
		this.delim = delim;
	}
}
