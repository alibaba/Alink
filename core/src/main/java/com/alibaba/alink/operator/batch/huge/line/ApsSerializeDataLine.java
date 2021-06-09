package com.alibaba.alink.operator.batch.huge.line;

import com.alibaba.alink.operator.common.aps.ApsSerializeData;

public class ApsSerializeDataLine extends ApsSerializeData <Number[]> {
	private static final long serialVersionUID = 8596677282831307242L;

	@Override
	protected String serilizeDataType(Number[] value) {
		if (value == null) {
			return null;
		}
		return value[0].toString() + "," + value[1].toString() + "," + value[2].toString();
	}

	@Override
	protected Number[] deserilizeDataType(String str) {
		String[] data = str.split(",");
		Number[] res = new Number[3];
		res[0] = Integer.valueOf(data[0]);
		res[1] = Integer.valueOf(data[1]);
		res[2] = Float.valueOf(data[2]);
		return res;
	}
}
