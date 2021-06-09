package com.alibaba.alink.operator.batch.huge.word2vec;

import com.alibaba.alink.operator.common.aps.ApsSerializeData;

import java.util.StringTokenizer;

public class ApsSerializeDataW2V extends ApsSerializeData <int[]> {
	private static final long serialVersionUID = 1644492278508791885L;

	@Override
	protected String serilizeDataType(int[] value) {
		if (value == null) {
			return null;
		}

		StringBuilder result = new StringBuilder();

		result.append(value.length);

		for (int val : value) {
			result.append(",");
			result.append(val);
		}

		return result.toString();
	}

	@Override
	protected int[] deserilizeDataType(String str) {
		if (str == null) {
			return null;
		}

		StringTokenizer stringTokenizer = new StringTokenizer(str, ",");

		if (!stringTokenizer.hasMoreTokens()) {
			return null;
		}

		int len = Integer.parseInt(stringTokenizer.nextToken());

		int[] ret = new int[len];

		for (int i = 0; i < len; ++i) {
			if (!stringTokenizer.hasMoreTokens()) {
				return null;
			}

			ret[i] = Integer.valueOf(stringTokenizer.nextToken());
		}

		return ret;
	}
}
