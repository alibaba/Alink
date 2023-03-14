package com.alibaba.alink.common.sql.builtin.agg;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import com.alibaba.alink.common.exceptions.AkUnsupportedOperationException;

import java.math.BigDecimal;

public class NumberTypeHandle {

	private TypeInformation dataType = null;

	NumberTypeHandle(Object data) {
		getType(data);
	}

	Number transformData(Number inputData) {
		if (Types.DOUBLE.equals(dataType)) {
			return inputData.doubleValue();
		} else if (Types.LONG.equals(dataType)) {
			return inputData.longValue();
		} else if (Types.INT.equals(dataType)) {
			return inputData.intValue();
		} else if (Types.FLOAT.equals(dataType)) {
			return inputData.floatValue();
		} else if (Types.SHORT.equals(dataType)) {
			return inputData.shortValue();
		} else if (Types.BYTE.equals(dataType)) {
			return inputData.byteValue();
		} else if (Types.BIG_DEC.equals(dataType)) {
			if (inputData instanceof BigDecimal) {
				return inputData;
			} else if (inputData instanceof Double || inputData instanceof Float) {
				return new BigDecimal(inputData.doubleValue());
			} else {
				return new BigDecimal(inputData.longValue());
			}
		}
		throw new AkUnsupportedOperationException("Do not support this type: " + dataType);
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
		} else if (data instanceof BigDecimal) {
			dataType = Types.BIG_DEC;
		} else {
			throw new AkUnsupportedOperationException("We only support double, long, int, float, short, byte.");
		}
	}
}
