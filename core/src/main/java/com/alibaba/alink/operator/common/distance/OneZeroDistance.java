package com.alibaba.alink.operator.common.distance;

import org.apache.flink.util.Preconditions;

import java.util.Objects;

public class OneZeroDistance implements CategoricalDistance {
	private static final long serialVersionUID = -6375080752955133016L;

	@Override
	public int calc(String str1, String str2) {
		return Objects.equals(str1, str2) ? 0 : 1;
	}

	@Override
	public int calc(String[] str1, String[] str2) {
		int distance = 0;
		Preconditions.checkArgument(str1.length == str2.length,
			"For OneZeroDistance, the categorical feature number must be equal!");
		for (int i = 0; i < str1.length; i++) {
			distance += calc(str1[i], str2[i]);
		}
		return distance;
	}
}
