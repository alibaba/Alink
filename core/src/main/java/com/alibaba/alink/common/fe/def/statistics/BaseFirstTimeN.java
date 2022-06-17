package com.alibaba.alink.common.fe.def.statistics;

public class BaseFirstTimeN implements BaseStatistics{
	int n;

	@Override
	public String name() {
		return "firstn_time_" + n;
	}

	public int getTimeN() {
		return n;
	}
}
