package com.alibaba.alink.operator.batch.huge.word2vec;

import com.alibaba.alink.operator.common.aps.ApsSerializeModel;

import java.util.StringTokenizer;

public class ApsSerializeModelW2V extends ApsSerializeModel <float[]> {
	private static final long serialVersionUID = -3312695975898065628L;

	@Override
	protected String serilizeModelType(float[] value) {
		if (value == null) {
			return null;
		}

		StringBuilder result = new StringBuilder();

		result.append(value.length);

		for (float val : value) {
			result.append(",");
			result.append(val);
		}

		return result.toString();
	}

	@Override
	protected float[] deserilizeModelType(String str) {
		if (str == null) {
			return null;
		}

		StringTokenizer stringTokenizer = new StringTokenizer(str, ",");

		if (!stringTokenizer.hasMoreTokens()) {
			return null;
		}

		int len = Integer.parseInt(stringTokenizer.nextToken());

		float[] ret = new float[len];

		for (int i = 0; i < len; ++i) {
			if (!stringTokenizer.hasMoreTokens()) {
				return null;
			}

			ret[i] = Float.valueOf(stringTokenizer.nextToken());
		}

		return ret;
	}
}
