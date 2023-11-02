package com.alibaba.alink.common.insights;

public class Subspace {

	public final String colName;

	public Object value;

	public Subspace(String colName, Object value) {
		this.colName = colName;
		this.value = value;
	}
}
