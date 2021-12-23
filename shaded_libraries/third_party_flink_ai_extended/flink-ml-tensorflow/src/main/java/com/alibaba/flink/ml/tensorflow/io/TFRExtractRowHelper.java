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

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tensorflow.example.Example;
import org.tensorflow.example.Feature;
import org.tensorflow.example.Features;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

/**
 * a helper function to convert TFRecord format record to row.
 * Example every feature corresponds to a column of row.
 */
public class TFRExtractRowHelper implements Serializable {

	public enum ScalarConverter {
		FIRST,
		LAST,
		MAX,
		MIN,
		ONE_HOT
	}

	private interface AggFunc<T, R> extends Serializable {
		R aggregate(List<T> values);
	}

	private static class FirstAgg<T> implements AggFunc<T, T> {

		@Override
		public T aggregate(List<T> values) {
			Preconditions.checkArgument(!values.isEmpty(), "Value list is empty");
			return values.get(0);
		}
	}

	private static class LastAgg<T> implements AggFunc<T, T> {

		@Override
		public T aggregate(List<T> values) {
			Preconditions.checkArgument(!values.isEmpty(), "Value list is empty");
			return values.get(values.size() - 1);
		}
	}

	private static class MaxAgg<T extends Comparable<T>> implements AggFunc<T, T> {

		@Override
		public T aggregate(List<T> values) {
			Preconditions.checkArgument(!values.isEmpty(), "Value list is empty");
			T max = null;
			for (T value : values) {
				if (max == null || max.compareTo(value) < 0) {
					max = value;
				}
			}
			return max;
		}
	}

	private static class MinAgg<T extends Comparable<T>> implements AggFunc<T, T> {

		@Override
		public T aggregate(List<T> values) {
			Preconditions.checkArgument(!values.isEmpty(), "Value list is empty");
			T min = null;
			for (T value : values) {
				if (min == null || min.compareTo(value) > 0) {
					min = value;
				}
			}
			return min;
		}
	}

	private static class OneHotAgg<T extends Number> implements AggFunc<T, Integer> {

		@Override
		public Integer aggregate(List<T> values) {
			Preconditions.checkArgument(!values.isEmpty(), "Value list is empty");
			int index = -1;
			for (int i = 0; i < values.size(); i++) {
				Number number = values.get(i);
				if (number.longValue() == 1) {
					Preconditions.checkArgument(index == -1, "Invalid one-hot list: " + values.toString());
					index = i;
				} else {
					Preconditions.checkArgument(number.longValue() == 0, "Invalid one-hot list: " + values.toString());
				}
			}
			Preconditions.checkArgument(index != -1, "Invalid one-hot list: " + values.toString());
			return index;
		}
	}

	private static final Logger LOG = LoggerFactory.getLogger(TFRExtractRowHelper.class);

	private final RowTypeInfo outputRowType;
	private final AggFunc[] aggs;

	private static AggFunc getAggFunc(ScalarConverter converter) {
		switch (converter) {
			case FIRST:
				return new FirstAgg();
			case LAST:
				return new LastAgg();
			case MAX:
				return new MaxAgg();
			case MIN:
				return new MinAgg();
			case ONE_HOT:
				return new OneHotAgg();
			default:
				throw new IllegalArgumentException("Unsupported converter " + converter);
		}
	}

	public TFRExtractRowHelper(RowTypeInfo outputRowType, ScalarConverter[] converters) {
		Preconditions.checkArgument(outputRowType.getArity() == converters.length);
		this.outputRowType = outputRowType;
		aggs = new AggFunc[converters.length];
		for (int i = 0; i < aggs.length; i++) {
			aggs[i] = getAggFunc(converters[i]);
		}
	}

	public Row extract(byte[] bytes) throws InvalidProtocolBufferException {
		Features features = Example.parseFrom(bytes).getFeatures();
		Preconditions.checkArgument(outputRowType.getArity() == features.getFeatureCount(),
				String.format("RowType arity (%d) and example feature count (%d) mismatch",
						outputRowType.getArity(), features.getFeatureCount()));
		Row res = new Row(outputRowType.getArity());
		for (int i = 0; i < outputRowType.getArity(); i++) {
			String name = outputRowType.getFieldNames()[i];
			Feature feature = Preconditions.checkNotNull(features.getFeatureOrDefault(name, null),
					String.format("Field name %s doesn't exist in example", name));
			res.setField(i, toObject(feature, outputRowType.getFieldTypes()[i], aggs[i]));
		}
		return res;
	}

	private Object toObject(Feature feature, TypeInformation dataType, AggFunc aggFunc) {
		final String typeMismatchError = String.format("Cannot convert %s to %s",
				feature.toString(), dataType.toString());
		final boolean isArray = dataType instanceof PrimitiveArrayTypeInfo;
		if (isArray) {
			dataType = ((PrimitiveArrayTypeInfo) dataType).getComponentType();
		}
		if (dataType.equals(Types.STRING())) {
			Preconditions.checkArgument(feature.hasBytesList(), typeMismatchError);
			String[] strings = new String[feature.getBytesList().getValueCount()];
			for (int i = 0; i < strings.length; i++) {
				strings[i] = feature.getBytesList().getValue(i).toString(StandardCharsets.ISO_8859_1);
			}
			if (isArray) {
				return strings;
			}
			return aggFunc.aggregate(Arrays.asList(strings));
		} else if (dataType.equals(Types.SHORT()) || dataType.equals(Types.INT()) || dataType.equals(Types.LONG())) {
			Preconditions.checkArgument(feature.hasInt64List(), typeMismatchError);
			long[] longs = feature.getInt64List().getValueList().stream().mapToLong(Long::valueOf).toArray();
			if (isArray) {
				if (dataType.equals(Types.SHORT())) {
					short[] shorts = new short[longs.length];
					for (int i = 0; i < shorts.length; i++) {
						shorts[i] = (short) longs[i];
					}
					return shorts;
				}
				if (dataType.equals(Types.INT())) {
					return Arrays.stream(longs).mapToInt(l -> Long.valueOf(l).intValue()).toArray();
				}
				return longs;
			}
			return aggFunc.aggregate(feature.getInt64List().getValueList());
		} else if (dataType.equals(Types.FLOAT())) {
			Preconditions.checkArgument(feature.hasFloatList(), typeMismatchError);
			float[] floats = new float[feature.getFloatList().getValueCount()];
			for (int i = 0; i < floats.length; i++) {
				floats[i] = feature.getFloatList().getValue(i);
			}
			if (isArray) {
				return floats;
			}
			return aggFunc.aggregate(feature.getFloatList().getValueList());
		} else if (dataType.equals(Types.PRIMITIVE_ARRAY(TypeInformation.of(byte[].class)))) {
			Preconditions.checkArgument(feature.hasBytesList(), typeMismatchError);
			byte[][] bytes = new byte[feature.getBytesList().getValueCount()][];
			for (int i = 0; i < bytes.length; i++) {
				bytes[i] = feature.getBytesList().getValue(i).toByteArray();
			}
			if (isArray) {
				return bytes;
			}
			ByteString byteString = (ByteString) aggFunc.aggregate(feature.getBytesList().getValueList());
			return byteString.toByteArray();
		}
		throw new IllegalArgumentException("Unsupported type " + dataType.toString());
	}
}
