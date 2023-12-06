package com.alibaba.alink.common.insights;

import java.io.Serializable;

public class Breakdown implements Serializable {

	public final String colName;

	public Breakdown(String colName) {
		this.colName = colName;
	}
}
