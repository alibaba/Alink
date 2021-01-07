package com.alibaba.alink.operator.common.io.types;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import com.alibaba.alink.common.VectorTypes;
import org.junit.Assert;
import org.junit.Test;

public class FlinkTypeConverterTest {

	private static final TypeInformation <?>[] TYPES = new TypeInformation <?>[] {
		//BasicTypeInfo.STRING_TYPE_INFO,
		//BasicTypeInfo.BOOLEAN_TYPE_INFO,
		//BasicTypeInfo.BYTE_TYPE_INFO,
		//BasicTypeInfo.SHORT_TYPE_INFO,
		//BasicTypeInfo.INT_TYPE_INFO,
		//BasicTypeInfo.LONG_TYPE_INFO,
		//BasicTypeInfo.FLOAT_TYPE_INFO,
		//BasicTypeInfo.DOUBLE_TYPE_INFO,
		//SqlTimeTypeInfo.DATE,
		//SqlTimeTypeInfo.TIME,
		//SqlTimeTypeInfo.TIMESTAMP,
		//BasicTypeInfo.BIG_DEC_TYPE_INFO,
		//Types.BIG_INT,
		//Types.BIG_DEC,
		//PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO,
		VectorTypes.VECTOR,
		VectorTypes.DENSE_VECTOR,
		VectorTypes.SPARSE_VECTOR
	};

	@Test
	public void testMutualConversion() {
		for (TypeInformation <?> type : TYPES) {
			String typeStr = FlinkTypeConverter.getTypeString(type);
			TypeInformation <?> flinkType = FlinkTypeConverter.getFlinkType(typeStr);
			Assert.assertEquals(type, flinkType);
		}

	}

	@Test
	public void testGetByArray() {
		String[] typeStr = FlinkTypeConverter.getTypeString(TYPES);
		for (int i = 0; i < typeStr.length; i++) {
			System.out.println(typeStr[i]);
			TypeInformation <?> type = FlinkTypeConverter.getFlinkType(typeStr[i]);
			Assert.assertEquals(TYPES[i], type);
		}
	}
}
