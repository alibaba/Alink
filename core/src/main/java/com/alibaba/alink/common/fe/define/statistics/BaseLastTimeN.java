package com.alibaba.alink.common.fe.define.statistics;

public class BaseLastTimeN implements BaseStatistics{
	int n;

	@Override
	public String name() {
		return "lastn_time_" + n;
	}

	public int getTimeN() {
		return n;
	}
}
