package com.alibaba.alink.common.fe.def.statistics;

public class BaseLastN implements BaseStatistics {
	int n;

	@Override
	public String name() {
		return "lastn_" + n;
	}

	public int getN() {
		return n;
	}
}
