package com.alibaba.alink.common.insights;

import java.io.Serializable;

public class Measure implements Serializable {

	public String colName;

	public MeasureAggr aggr;

	public Measure() {

	}

	public Measure(String colName, MeasureAggr aggr) {
		this.colName = colName;
		this.aggr = aggr;
	}

	public String getColName() {
		return colName;
	}

	public void setColName(String colName) {
		this.colName = colName;
	}

	public MeasureAggr getAggr() {
		return aggr;
	}

	public void setAggr(MeasureAggr aggr) {
		this.aggr = aggr;
	}

	@Override
	public String toString() {
		return new StringBuilder().append(this.colName).append(aggr.toString()).toString();
	}
}
