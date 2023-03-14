package com.alibaba.alink.common.dl.utils;

import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.type.AlinkTypes;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Assert;
import org.junit.Test;
import org.tensorflow.proto.example.Example;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;

/**
 * Only check feature number and names here. Feature content test is in {@link TFFeatureConversionTest}
 */
public class TFExampleConversionTest extends AlinkTestBase {
	@Test
	public void testFromToExample() {
		Object[][] values = new Object[][] {
			new Object[] {"f", 1.f, AlinkTypes.FLOAT},
			new Object[] {"d", 1., AlinkTypes.DOUBLE},
			new Object[] {"l", 1L, AlinkTypes.LONG},
			new Object[] {"i", 1, AlinkTypes.INT},
			new Object[] {"s", "abc", AlinkTypes.STRING},
			new Object[] {"bs", "abc".getBytes(StandardCharsets.UTF_8),
				PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO},
		};
		int n = values.length;
		Row row = new Row(values.length);
		for (int i = 0; i < n; i += 1) {
			row.setField(i, values[i][1]);
		}
		String[] names = Arrays.stream(values)
			.map(d -> (String) d[0])
			.toArray(String[]::new);

		TypeInformation <?>[] types = Arrays.stream(values)
			.map(d -> (TypeInformation <?>) d[2])
			.toArray(TypeInformation <?>[]::new);
		Example example = TFExampleConversionUtils.toExample(row, names, types);
		Assert.assertEquals(example.getFeatures().getFeatureMap().keySet(), new HashSet <>(Arrays.asList(names)));

		Row fromExample = TFExampleConversionUtils.fromExample(example, names, types);
		Assert.assertEquals(n, fromExample.getArity());
	}
}
