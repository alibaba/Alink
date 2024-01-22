package com.alibaba.alink.common.insights;

import java.io.Serializable;

public class Breakdown implements Serializable {

	public String colName;

	public Breakdown() {

	}

	public Breakdown(String colName) {
		this.colName = colName;
	}

	public String getColName() {
		return colName;
	}

	public void setColName(String colName) {
		this.colName = colName;
	}
}
