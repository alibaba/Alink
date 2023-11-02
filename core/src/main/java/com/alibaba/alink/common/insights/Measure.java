package com.alibaba.alink.common.insights;

public class Measure {

	public final String colName;

	public final MeasureAggr aggr;

	public Measure(String colName, MeasureAggr aggr) {
		this.colName = colName;
		this.aggr = aggr;
	}
}
