package com.alibaba.alink.operator.common.feature.quantile;

import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

public final class PairComparableTypeInfoFactory extends TypeInfoFactory <PairComparable> {
	@Override
	public TypeInformation <PairComparable> createTypeInfo(Type t,
														   Map <String, TypeInformation <?>> genericParameters) {
		Map <String, TypeInformation <?>> fields = new HashMap <>();
		fields.put("first", Types.INT);
		fields.put("second", NumericType.NUMERIC_TYPE);
		return Types.POJO(PairComparable.class, fields);
	}
}
