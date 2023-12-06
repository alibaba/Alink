package com.alibaba.alink.common.insights;

public enum MeasureAggr {
	COUNT("个数", "COUNT"),
	AVG("平均值", "AVG"),
	SUM("求和", "SUM"),
	MAX("最大值", "MAX"),
	MIN("最小值", "MIN");

	private final String cnName;
	private final String enName;

	private MeasureAggr(String cnName, String enName) {
		this.cnName = cnName;
		this.enName = enName;
	}

	public String getCnName() {
		return this.cnName;
	}

	public String getEnName() {
		return this.enName;
	}

}
