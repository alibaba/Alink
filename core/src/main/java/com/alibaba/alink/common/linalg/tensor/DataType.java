package com.alibaba.alink.common.linalg.tensor;

import java.util.HashMap;
import java.util.Map;

public enum DataType {
	FLOAT(1),
	DOUBLE(2),
	INT(3),
	LONG(4),
	BOOLEAN(5),
	BYTE(6),
	UBYTE(7),
	STRING(8);

	static final Map <Integer, DataType> m = new HashMap <>();

	private final int index;

	static {
		for (DataType value : DataType.values()) {
			m.put(value.index, value);
		}
	}

	DataType(int index) {
		this.index = index;
	}

	public int getIndex() {
		return index;
	}

	public static DataType fromIndex(int index) {
		return m.get(index);
	}
}
