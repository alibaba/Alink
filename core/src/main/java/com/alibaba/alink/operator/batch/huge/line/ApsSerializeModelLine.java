package com.alibaba.alink.operator.batch.huge.line;

import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.common.aps.ApsSerializeModel;

public class ApsSerializeModelLine extends ApsSerializeModel <float[][]> {
	private static final long serialVersionUID = -8029444346646052598L;

	@Override
	protected String serilizeModelType(float[][] value) {
		return JsonConverter.toJson(value);
	}

	@Override
	protected float[][] deserilizeModelType(String str) {
		return JsonConverter.fromJson(str, float[][].class);
	}
}
