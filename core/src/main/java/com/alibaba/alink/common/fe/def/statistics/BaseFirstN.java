package com.alibaba.alink.common.fe.def.statistics;

public class BaseFirstN implements BaseStatistics {
	int n;

	@Override
	public String name() {
		return "firstn_" + n;
	}

	public int getN() {
		return n;
	}
}
