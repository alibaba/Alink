package com.alibaba.alink.common.insights;

import java.io.Serializable;

public class Measure implements Serializable {

	public final String colName;

	public final MeasureAggr aggr;

	public Measure(String colName, MeasureAggr aggr) {
		this.colName = colName;
		this.aggr = aggr;
	}

	@Override
	public String toString() {
		return new StringBuilder().append(this.colName).append(aggr.toString()).toString();
	}
}
