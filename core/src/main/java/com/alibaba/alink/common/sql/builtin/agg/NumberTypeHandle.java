package com.alibaba.alink.common.sql.builtin.agg;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

public class NumberTypeHandle {


	private TypeInformation dataType = null;
	NumberTypeHandle(Object data) {
		getType(data);
	}

	Number transformData(Number inputData) {
		Double data = inputData.doubleValue();
		if (Types.LONG.equals(dataType)) {
			return data.longValue();
		} else if (Types.INT.equals(dataType)) {
			return data.intValue();
		} else if (Types.SHORT.equals(dataType)) {
			return data.shortValue();
		} else if (Types.FLOAT.equals(dataType)) {
			return data.floatValue();
		} else if (Types.BYTE.equals(dataType)) {
			return data.byteValue();
		} else if (Types.DOUBLE.equals(dataType)) {
			return data;
		}
		throw new RuntimeException("Do not support this type: " + dataType);
	}

	private void getType(Object data) {
		if (data instanceof Double) {
			dataType = Types.DOUBLE;
		} else if (data instanceof Long) {
			dataType = Types.LONG;
		} else if (data instanceof Integer) {
			dataType = Types.INT;
		} else if (data instanceof Short) {
			dataType = Types.SHORT;
		} else if (data instanceof Float) {
			dataType = Types.FLOAT;
		} else if (data instanceof Byte) {
			dataType = Types.BYTE;
		} else {
			throw new RuntimeException("We only support double, long, int, float, short, byte.");
		}
	}
}
