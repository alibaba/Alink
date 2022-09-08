package com.alibaba.alink.common.fe.define.statistics;

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
