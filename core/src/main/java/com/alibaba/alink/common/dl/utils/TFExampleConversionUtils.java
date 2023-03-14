package com.alibaba.alink.common.dl.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.type.AlinkTypes;
import com.alibaba.alink.common.dl.coding.TFExampleConversionV2;
import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.common.exceptions.AkPreconditions;
import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.tensor.BoolTensor;
import com.alibaba.alink.common.linalg.tensor.ByteTensor;
import com.alibaba.alink.common.linalg.tensor.DoubleTensor;
import com.alibaba.alink.common.linalg.tensor.FloatTensor;
import com.alibaba.alink.common.linalg.tensor.IntTensor;
import com.alibaba.alink.common.linalg.tensor.LongTensor;
import com.alibaba.alink.common.linalg.tensor.Shape;
import com.alibaba.alink.common.linalg.tensor.StringTensor;
import com.alibaba.alink.common.linalg.tensor.TensorInternalUtils;
import com.alibaba.alink.common.linalg.tensor.UByteTensor;
import com.alibaba.flink.ml.tf2.shaded.com.google.protobuf.ByteString;
import org.apache.commons.lang3.ArrayUtils;
import org.tensorflow.ndarray.ByteNdArray;
import org.tensorflow.ndarray.StdArrays;
import org.tensorflow.proto.example.BytesList;
import org.tensorflow.proto.example.BytesList.Builder;
import org.tensorflow.proto.example.Example;
import org.tensorflow.proto.example.Feature;
import org.tensorflow.proto.example.Feature.KindCase;
import org.tensorflow.proto.example.Features;
import org.tensorflow.proto.example.FloatList;
import org.tensorflow.proto.example.Int64List;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Convert from/to TensorFlow Example and Feature.
 * <p>
 * Not use {@link TFExampleConversionV2}, because Flink-ai-extended defined too many less common types.
 */
public class TFExampleConversionUtils {

	static final List <TypeInformation <?>> TO_FLOAT_TYPES = Arrays.asList(
		AlinkTypes.DOUBLE, AlinkTypes.FLOAT, AlinkTypes.BIG_DEC
	);

	static final List <TypeInformation <?>> TO_LONG_TYPES = Arrays.asList(
		AlinkTypes.LONG, AlinkTypes.INT, AlinkTypes.BIG_INT, AlinkTypes.SHORT
	);

	/**
	 * Convert a Java object to {@link Feature}. Following conversion rules are applied:
	 * <li>{@link TFExampleConversionUtils#TO_FLOAT_TYPES}: to float feature</li>
	 * <li>{@link TFExampleConversionUtils#TO_LONG_TYPES}: to int64 feature</li>
	 * <li>String: to bytes feature</li>
	 * <li>DenseVector: to float feature</li>
	 * <li>FloatTensor/DoubleTensor: to float feature, flattened</li>
	 * <li>IntTensor/LongTensor: to int64 feature, flattened</li>
	 * <li>ByteTensor: to bytes feature, only support rank = 1 and rank = 2, not flattened</li>
	 * <li>StringTensor: to bytes feature, flattened</li>
	 * <li>byte[]: to bytes features</li>
	 *
	 * @param val  the Java object
	 * @param type the type of this object. Unmatched type will give undefined result.
	 * @return {@link Feature}
	 */
	public static Feature toFeature(Object val, TypeInformation <?> type) {
		Feature.Builder featureBuilder = Feature.newBuilder();
		FloatList.Builder floatListBuilder = FloatList.newBuilder();
		Int64List.Builder int64ListBuilder = Int64List.newBuilder();

		// When type is `TENSOR`, find the exact type.
		if (AlinkTypes.TENSOR.equals(type)) {
			if (val instanceof FloatTensor) {
				type = AlinkTypes.FLOAT_TENSOR;
			} else if (val instanceof DoubleTensor) {
				type = AlinkTypes.DOUBLE_TENSOR;
			} else if (val instanceof IntTensor) {
				type = AlinkTypes.INT_TENSOR;
			} else if (val instanceof LongTensor) {
				type = AlinkTypes.LONG_TENSOR;
			} else if (val instanceof BoolTensor) {
				type = AlinkTypes.BOOL_TENSOR;
			} else if (val instanceof UByteTensor) {
				type = AlinkTypes.UBYTE_TENSOR;
			} else if (val instanceof StringTensor) {
				type = AlinkTypes.STRING_TENSOR;
			} else if (val instanceof ByteTensor) {
				type = AlinkTypes.BYTE_TENSOR;
			}
		} else if (AlinkTypes.VECTOR.equals(type)) {
			type = AlinkTypes.DENSE_VECTOR;
			if (val instanceof SparseVector) {
				val = ((SparseVector) val).toDenseVector();
			}
		}

		if (TO_FLOAT_TYPES.contains(type)) {
			floatListBuilder.addValue(((Number) val).floatValue());
			featureBuilder.setFloatList(floatListBuilder);
		} else if (TO_LONG_TYPES.contains(type)) {
			int64ListBuilder.addValue(((Number) val).longValue());
			featureBuilder.setInt64List(int64ListBuilder);
		} else if (AlinkTypes.STRING.equals(type)) {
			Builder bb = BytesList.newBuilder();
			bb.addValue(ByteString.copyFrom((String) val, StandardCharsets.UTF_8));
			featureBuilder.setBytesList(bb);
		} else if (AlinkTypes.DENSE_VECTOR.equals(type)) {
			List <Float> floatList = Arrays.stream(((DenseVector) val).getData())
				.mapToObj(d -> (float) d)
				.collect(Collectors.toList());
			floatListBuilder.addAllValue(floatList);
		} else if (AlinkTypes.FLOAT_TENSOR.equals(type)) {
			FloatTensor floatTensor = (FloatTensor) val;
			long size = floatTensor.size();
			floatTensor = floatTensor.reshape(new Shape(size));
			for (long i = 0; i < size; i += 1) {
				floatListBuilder.addValue(floatTensor.getFloat(i));
			}
			featureBuilder.setFloatList(floatListBuilder);
		} else if (AlinkTypes.DOUBLE_TENSOR.equals(type)) {
			DoubleTensor doubleTensor = (DoubleTensor) val;
			long size = doubleTensor.size();
			doubleTensor = doubleTensor.reshape(new Shape(size));
			for (long i = 0; i < size; i += 1) {
				floatListBuilder.addValue((float) doubleTensor.getDouble(i));
			}
			featureBuilder.setFloatList(floatListBuilder);
		} else if (AlinkTypes.INT_TENSOR.equals(type)) {
			IntTensor intTensor = (IntTensor) val;
			long size = intTensor.size();
			intTensor = intTensor.reshape(new Shape(size));
			for (long i = 0; i < size; i += 1) {
				int64ListBuilder.addValue(intTensor.getInt(i));
			}
			featureBuilder.setInt64List(int64ListBuilder);
		} else if (AlinkTypes.LONG_TENSOR.equals(type)) {
			LongTensor longTensor = (LongTensor) val;
			long size = longTensor.size();
			longTensor = longTensor.reshape(new Shape(size));
			for (long i = 0; i < size; i += 1) {
				int64ListBuilder.addValue(longTensor.getLong(i));
			}
			featureBuilder.setInt64List(int64ListBuilder);
		} else if (AlinkTypes.BYTE_TENSOR.equals(type)) {
			ByteTensor byteTensor = (ByteTensor) val;
			long[] shape = byteTensor.shape();
			ByteNdArray data = (ByteNdArray) TensorInternalUtils.getTensorData(byteTensor);
			BytesList.Builder bb = BytesList.newBuilder();
			if (shape.length == 1) {
				byte[] bytes = StdArrays.array1dCopyOf(data);
				bb.addValue(ByteString.copyFrom(bytes));
			} else if (shape.length == 2) {
				byte[][] bytes = StdArrays.array2dCopyOf(data);
				List <ByteString> byteStringList = Arrays.stream(bytes)
					.map(ByteString::copyFrom)
					.collect(Collectors.toList());
				bb.addAllValue(byteStringList);
			} else {
				throw new AkUnsupportedOperationException("Not support ByteTensor with rank > 2");
			}
			featureBuilder.setBytesList(bb);
		} else if (AlinkTypes.STRING_TENSOR.equals(type)) {
			StringTensor stringTensor = (StringTensor) val;
			long size = stringTensor.size();
			stringTensor = stringTensor.reshape(new Shape(size));
			Builder bb = BytesList.newBuilder();
			for (long i = 0; i < size; i += 1) {
				bb.addValue(ByteString.copyFrom(stringTensor.getString(i), StandardCharsets.UTF_8));
			}
			featureBuilder.setBytesList(bb);
		} else if (AlinkTypes.VARBINARY.equals(type)) {
			Builder bb = BytesList.newBuilder();
			bb.addValue(ByteString.copyFrom((byte[]) val));
			featureBuilder.setBytesList(bb);
		} else {
			throw new AkUnsupportedOperationException(String.format("Unsupported data type for TF: %s", type));
		}
		return featureBuilder.build();
	}

	/**
	 * Convert a `feature` to a Java object of given `type`. Following conversion rules are supported:
	 * <li>FloatTensor/DoubleTensor: from float feature</li>
	 * <li>LongTensor/IntTensor: from int64 feature</li>
	 * <li>StringTensor: from bytes feature</li>
	 * <li>ByteTensor: from bytes feature</li>
	 * <li>DenseVector: from float feature</li>
	 * <li>Long/Int: from int64 feature, only 1st entry</li>
	 * <li>Float/Double: from float feature, only 1st entry</li>
	 * <li>String: from bytes feature, only 1st entry</li>
	 * <li>byte[]: from bytes feature</li>
	 *
	 * @param feature feature
	 * @param type    give ntype
	 * @return a Java object
	 */
	public static Object fromFeature(Feature feature, TypeInformation <?> type) {
		KindCase kindCase = feature.getKindCase();
		List <Float> floatList = feature.getFloatList().getValueList();
		List <Long> longList = feature.getInt64List().getValueList();
		List <ByteString> byteStringList = feature.getBytesList().getValueList();
		float[] floats = ArrayUtils.toPrimitive(floatList.toArray(new Float[0]));
		long[] longs = ArrayUtils.toPrimitive(longList.toArray(new Long[0]));
		int[] ints = longList.stream().mapToInt(Long::intValue).toArray();

		if (AlinkTypes.isTensorType(type)) {    // Tensor
			if (AlinkTypes.FLOAT_TENSOR.equals(type)) {
				AkPreconditions.checkArgument(floats.length > 0,
					new AkIllegalDataException("no FLOAT values in the feature."));
				return new FloatTensor(floats);
			} else if (AlinkTypes.DOUBLE_TENSOR.equals(type)) {
				AkPreconditions.checkArgument(floats.length > 0,
					new AkIllegalDataException("no FLOAT values in the feature."));
				return DoubleTensor.of(new FloatTensor(floats));
			} else if (AlinkTypes.LONG_TENSOR.equals(type)) {
				AkPreconditions.checkArgument(longs.length > 0,
					new AkIllegalDataException("no INT64 values in the feature."));
				return new LongTensor(longs);
			} else if (AlinkTypes.INT_TENSOR.equals(type)) {
				AkPreconditions.checkArgument(ints.length > 0,
					new AkIllegalDataException("no INT64 values in the feature."));
				return new IntTensor(ints);
			} else if (AlinkTypes.STRING_TENSOR.equals(type)) {
				AkPreconditions.checkArgument(byteStringList.size() > 0,
					new AkIllegalDataException("no BYTES values in the feature."));
				String[] strings = byteStringList.stream()
					.map(d -> d.toString(StandardCharsets.UTF_8))
					.toArray(String[]::new);
				return new StringTensor(strings);
			} else if (AlinkTypes.BYTE_TENSOR.equals(type)) {
				AkPreconditions.checkArgument(byteStringList.size() > 0,
					new AkIllegalDataException("no BYTES values in the feature."));
				byte[][] bytes = byteStringList.stream()
					.map(ByteString::toByteArray)
					.toArray(byte[][]::new);
				return new ByteTensor(bytes);
			}
		} else if (AlinkTypes.isVectorType(type)) {    // Vector
			if (AlinkTypes.DENSE_VECTOR.equals(type)) {
				AkPreconditions.checkArgument(floats.length > 0,
					new AkIllegalDataException("no FLOAT values in the feature."));
				return DoubleTensor.of(new FloatTensor(floats)).toVector();
			}
		} else {    // Primitives
			if (AlinkTypes.LONG.equals(type)) {
				AkPreconditions.checkArgument(longs.length > 0,
					new AkIllegalDataException("no INT64 values in the feature."));
				return longs[0];
			} else if (AlinkTypes.INT.equals(type)) {
				AkPreconditions.checkArgument(longs.length > 0,
					new AkIllegalDataException("no INT64 values in the feature."));
				return (int) longs[0];
			} else if (AlinkTypes.FLOAT.equals(type)) {
				AkPreconditions.checkArgument(floats.length > 0,
					new AkIllegalDataException("no FLOAT values in the feature."));
				return floats[0];
			} else if (AlinkTypes.DOUBLE.equals(type)) {
				AkPreconditions.checkArgument(floats.length > 0,
					new AkIllegalDataException("no FLOAT values in the feature."));
				return (double) floats[0];
			} else if (AlinkTypes.STRING.equals(type)) {
				AkPreconditions.checkArgument(byteStringList.size() > 0,
					new AkIllegalDataException("no BYTES values in the feature."));
				return byteStringList.get(0).toString(StandardCharsets.UTF_8);
			} else if (AlinkTypes.VARBINARY.equals(type)) {
				AkPreconditions.checkArgument(byteStringList.size() > 0,
					new AkIllegalDataException("no BYTES values in the feature."));
				return byteStringList.get(0).toByteArray();
			}
		}
		throw new AkUnsupportedOperationException(
			String.format("Feature of type %s cannot convert to Java object of type %s. Support "
					+ "FLOAT feature to Float(Tensor), Double(Tensor), and DenseVector; "
					+ "LONG feature to Long(Tensor); STRING feature to String(Tensor)."
				, kindCase, type));
	}

	public static Row fromExample(Example example, String[] names, TypeInformation <?>[] types) {
		AkPreconditions.checkArgument(names.length == types.length);
		Map <String, Feature> featureMap = example.getFeatures().getFeatureMap();
		Row row = new Row(names.length);
		for (int i = 0; i < names.length; i++) {
			String name = names[i];
			if (!featureMap.containsKey(name)) {
				throw new AkIllegalDataException(String.format("No feature named %s in the example.", name));
			}
			Object obj = fromFeature(featureMap.get(name), types[i]);
			row.setField(i, obj);
		}
		return row;
	}

	public static Example toExample(Row row, String[] names, TypeInformation <?>[] types) {
		Example.Builder exampleBuilder = Example.newBuilder();
		Features.Builder featuresBuilder = exampleBuilder.getFeaturesBuilder();
		for (int i = 0; i < names.length; i++) {
			Feature feature = toFeature(row.getField(i), types[i]);
			featuresBuilder.putFeature(names[i], feature);
		}
		return exampleBuilder.build();
	}
}
