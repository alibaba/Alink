package com.alibaba.alink.common.dl.utils;

import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import com.alibaba.alink.common.AlinkTypes;
import com.alibaba.alink.common.linalg.tensor.ByteTensor;
import com.alibaba.alink.common.linalg.tensor.DoubleTensor;
import com.alibaba.alink.common.linalg.tensor.FloatTensor;
import com.alibaba.alink.common.linalg.tensor.IntTensor;
import com.alibaba.alink.common.linalg.tensor.LongTensor;
import com.alibaba.alink.common.linalg.tensor.Shape;
import com.alibaba.alink.common.linalg.tensor.StringTensor;
import com.alibaba.alink.common.linalg.tensor.Tensor;
import com.alibaba.flink.ml.tf2.shaded.com.google.protobuf.ByteString;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Longs;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.tensorflow.proto.example.BytesList;
import org.tensorflow.proto.example.Feature;
import org.tensorflow.proto.example.FloatList;
import org.tensorflow.proto.example.Int64List;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

@RunWith(Parameterized.class)
public class TFFeatureConversionTest {
	private final Object val;
	private final TypeInformation <?> type;
	private final Feature feature;

	private static Feature buildFeature(float... values) {
		return Feature.newBuilder()
			.setFloatList(FloatList.newBuilder().addAllValue(Floats.asList(values)))
			.build();
	}

	private static Feature buildFeature(long... values) {
		return Feature.newBuilder()
			.setInt64List(Int64List.newBuilder().addAllValue(Longs.asList(values)))
			.build();
	}

	private static Feature buildFeature(byte[]... values) {
		List <ByteString> byteStringList = Arrays.stream(values)
			.map(ByteString::copyFrom)
			.collect(Collectors.toList());
		return Feature.newBuilder()
			.setBytesList(BytesList.newBuilder().addAllValue(byteStringList))
			.build();
	}

	private static Feature buildFeature(String... values) {
		List <ByteString> byteStringList = Arrays.stream(values)
			.map(d -> ByteString.copyFrom(d, StandardCharsets.UTF_8))
			.collect(Collectors.toList());
		return Feature.newBuilder()
			.setBytesList(BytesList.newBuilder().addAllValue(byteStringList))
			.build();
	}

	@Parameters
	public static Collection <Object[]> data() {
		return Arrays.asList(
			new Object[] {1.f, AlinkTypes.FLOAT, buildFeature(1.f)},
			new Object[] {1., AlinkTypes.DOUBLE, buildFeature(1.f)},
			new Object[] {1, AlinkTypes.INT, buildFeature(1L)},
			new Object[] {1L, AlinkTypes.LONG, buildFeature(1L)},
			new Object[] {"abc", AlinkTypes.STRING, buildFeature("abc")},
			new Object[] {new byte[] {1, 2, 3}, PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO,
				buildFeature(new byte[] {1, 2, 3})},
			new Object[] {new FloatTensor(new float[][] {{1.f, 2.f}, {3.f, 4.f}}), AlinkTypes.FLOAT_TENSOR,
				buildFeature(1.f, 2.f, 3.f, 4.f)},
			new Object[] {new DoubleTensor(new double[][] {{1., 2.}, {3., 4.}}), AlinkTypes.DOUBLE_TENSOR,
				buildFeature(1.f, 2.f, 3.f, 4.f)},
			new Object[] {new LongTensor(new long[][] {{1L, 2L}, {3L, 4L}}), AlinkTypes.LONG_TENSOR,
				buildFeature(1, 2, 3, 4)},
			new Object[] {new IntTensor(new int[][] {{1, 2}, {3, 4}}), AlinkTypes.INT_TENSOR,
				buildFeature(1, 2, 3, 4)},
			new Object[] {new StringTensor(new String[][] {{"ab", "cd"}, {"ef", "gh"}}), AlinkTypes.STRING_TENSOR,
				buildFeature("ab", "cd", "ef", "gh")},
			new Object[] {new ByteTensor(new byte[] {1, 2}), AlinkTypes.BYTE_TENSOR,
				buildFeature(new byte[] {1, 2})},
			new Object[] {new ByteTensor(new byte[][] {{1, 2}, {3, 4}}), AlinkTypes.BYTE_TENSOR,
				buildFeature(new byte[] {1, 2}, new byte[] {3, 4})}
		);
	}

	public TFFeatureConversionTest(Object val, TypeInformation <?> type, Feature feature) {
		this.val = val;
		this.type = type;
		this.feature = feature;
	}

	@Test
	public void testToFeature() {
		Assert.assertEquals(feature, TFExampleConversionUtils.toFeature(val, type));
	}

	@Test
	public void testFromFeature() {
		if (val instanceof ByteTensor) {
			ByteTensor byteTensor = (ByteTensor) val;
			long[] shape = byteTensor.shape();
			if (shape.length == 1) {
				Assert.assertEquals((byteTensor).reshape(new Shape(1, shape[0])),
					TFExampleConversionUtils.fromFeature(feature, type));
			} else {
				Assert.assertEquals(val, TFExampleConversionUtils.fromFeature(feature, type));
			}
		} else if (val instanceof Tensor) {
			Assert.assertEquals(((Tensor <?>) val).flatten(), TFExampleConversionUtils.fromFeature(feature, type));
		} else if (val instanceof byte[]) {
			Assert.assertArrayEquals((byte[]) val, (byte[]) TFExampleConversionUtils.fromFeature(feature, type));
		} else {
			Assert.assertEquals(val, TFExampleConversionUtils.fromFeature(feature, type));
		}
	}
}
